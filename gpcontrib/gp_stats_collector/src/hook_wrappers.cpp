/*-------------------------------------------------------------------------
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 *
 * hook_wrappers.cpp
 *
 * IDENTIFICATION
 *	  gpcontrib/gp_stats_collector/src/hook_wrappers.cpp
 *
 *-------------------------------------------------------------------------
 */

#define typeid __typeid
extern "C" {
#include "postgres.h"
#include "cdb/cdbvars.h"
#include "cdb/ml_ipc.h"
#include "executor/execUtils.h"
#include "executor/executor.h"
#include "funcapi.h"
#include "stat_statements_parser/pg_stat_statements_parser.h"
#include "tcop/utility.h"
#include "utils/builtins.h"
#include "utils/elog.h"
#include "utils/metrics_utils.h"

#include <errno.h>
#include <poll.h>
#include <sys/socket.h>
#include <sys/un.h>
#include <unistd.h>
}
#undef typeid

#include "Config.h"
#include "EventSender.h"
#include "GpscStat.h"
#include "hook_wrappers.h"
#include "memory/gpdbwrappers.h"
#include "pg_query_state/pg_query_state.h"

static ExecutorStart_hook_type previous_ExecutorStart_hook = nullptr;
static ExecutorRun_hook_type previous_ExecutorRun_hook = nullptr;
static ExecutorFinish_hook_type previous_ExecutorFinish_hook = nullptr;
static ExecutorEnd_hook_type previous_ExecutorEnd_hook = nullptr;
static query_info_collect_hook_type previous_query_info_collect_hook = nullptr;
#ifdef ANALYZE_STATS_COLLECT_HOOK
static analyze_stats_collect_hook_type previous_analyze_stats_collect_hook =
	nullptr;
#endif
#ifdef IC_TEARDOWN_HOOK
static ic_teardown_hook_type previous_ic_teardown_hook = nullptr;
#endif
static ProcessUtility_hook_type previous_ProcessUtility_hook = nullptr;

static void gpsc_ExecutorStart_hook(QueryDesc *query_desc, int eflags);
static void gpsc_ExecutorRun_hook(QueryDesc *query_desc,
								  ScanDirection direction, uint64 count,
								  bool execute_once);
static void gpsc_ExecutorFinish_hook(QueryDesc *query_desc);
static void gpsc_ExecutorEnd_hook(QueryDesc *query_desc);
static void gpsc_query_info_collect_hook(QueryMetricsStatus status, void *arg);
#ifdef IC_TEARDOWN_HOOK
static void gpsc_ic_teardown_hook(ChunkTransportState *transportStates,
								  bool hasErrors);
#endif
#ifdef ANALYZE_STATS_COLLECT_HOOK
static void gpsc_analyze_stats_collect_hook(QueryDesc *query_desc);
#endif
static void gpsc_process_utility_hook(
	PlannedStmt *pstmt, const char *queryString, bool readOnlyTree,
	ProcessUtilityContext context, ParamListInfo params,
	QueryEnvironment *queryEnv, DestReceiver *dest, QueryCompletion *qc);

#define TEST_MAX_CONNECTIONS 4
#define TEST_RCV_BUF_SIZE 8192
#define TEST_POLL_TIMEOUT_MS 200

static int test_server_fd = -1;
static char *test_sock_path = NULL;

static EventSender *sender = nullptr;

/*
 * get_sender -- lazily construct the per-backend EventSender instance.
 */
static inline EventSender *
get_sender()
{
	if (!sender)
		sender = new EventSender();
	return sender;
}

/*
 * cpp_call -- invoke a C++ member function, converting exceptions to ereport.
 *
 * Wraps obj->*func(args...) in a try/catch so that C++ exceptions thrown
 * inside EventSender methods are converted to PostgreSQL ERROR instead of
 * crashing the backend with an unhandled exception.
 */
template <typename T, typename R, typename... Args>
R
cpp_call(T *obj, R (T::*func)(Args...), Args... args)
{
	try
	{
		return (obj->*func)(args...);
	}
	catch (const std::exception &e)
	{
		ereport(ERROR, (errmsg("Unexpected exception in gpsc: %s", e.what())));
		pg_unreachable();
	}
}

/*
 * hooks_init -- install all executor and utility hooks.
 *
 * Called from _PG_init() for QD and QE roles.  Initialises the GUC registry,
 * the GpscStat shared-memory statistics counters, and the stat-statements
 * parser, then chains each hook onto the existing hook pointer.
 */
void
hooks_init()
{
	Config::init_gucs();
	GpscStat::init();
	previous_ExecutorStart_hook = ExecutorStart_hook;
	ExecutorStart_hook = gpsc_ExecutorStart_hook;
	previous_ExecutorRun_hook = ExecutorRun_hook;
	ExecutorRun_hook = gpsc_ExecutorRun_hook;
	previous_ExecutorFinish_hook = ExecutorFinish_hook;
	ExecutorFinish_hook = gpsc_ExecutorFinish_hook;
	previous_ExecutorEnd_hook = ExecutorEnd_hook;
	ExecutorEnd_hook = gpsc_ExecutorEnd_hook;
	previous_query_info_collect_hook = query_info_collect_hook;
	query_info_collect_hook = gpsc_query_info_collect_hook;
#ifdef IC_TEARDOWN_HOOK
	previous_ic_teardown_hook = ic_teardown_hook;
	ic_teardown_hook = gpsc_ic_teardown_hook;
#endif
#ifdef ANALYZE_STATS_COLLECT_HOOK
	previous_analyze_stats_collect_hook = analyze_stats_collect_hook;
	analyze_stats_collect_hook = gpsc_analyze_stats_collect_hook;
#endif
	stat_statements_parser_init();
	previous_ProcessUtility_hook = ProcessUtility_hook;
	ProcessUtility_hook = gpsc_process_utility_hook;
}

/*
 * hooks_deinit -- restore all hooks to their previous values.
 *
 * Called from _PG_fini().  Cleans up the EventSender and GpscStat resources.
 */
void
hooks_deinit()
{
	ExecutorStart_hook = previous_ExecutorStart_hook;
	ExecutorEnd_hook = previous_ExecutorEnd_hook;
	ExecutorRun_hook = previous_ExecutorRun_hook;
	ExecutorFinish_hook = previous_ExecutorFinish_hook;
	query_info_collect_hook = previous_query_info_collect_hook;
#ifdef IC_TEARDOWN_HOOK
	ic_teardown_hook = previous_ic_teardown_hook;
#endif
#ifdef ANALYZE_STATS_COLLECT_HOOK
	analyze_stats_collect_hook = previous_analyze_stats_collect_hook;
#endif
	stat_statements_parser_deinit();
	if (sender)
	{
		delete sender;
		sender = nullptr;
	}
	GpscStat::deinit();
	ProcessUtility_hook = previous_ProcessUtility_hook;
}

/*
 * gpsc_ExecutorStart_hook -- wrapper for ExecutorStart.
 *
 * Notifies pg_query_state of the new query (enables instrumentation) and
 * calls the EventSender before and after the actual ExecutorStart.
 */
void
gpsc_ExecutorStart_hook(QueryDesc *query_desc, int eflags)
{
	pg_qs_executor_start(query_desc, eflags);

	cpp_call(get_sender(), &EventSender::executor_before_start, query_desc,
			 eflags);

	if (previous_ExecutorStart_hook)
		(*previous_ExecutorStart_hook)(query_desc, eflags);
	else
		standard_ExecutorStart(query_desc, eflags);

	cpp_call(get_sender(), &EventSender::executor_after_start, query_desc,
			 eflags);
}

/*
 * gpsc_ExecutorRun_hook -- wrapper for ExecutorRun.
 *
 * Pushes the QueryDesc onto the pg_query_state stack so signal handlers can
 * find it, then pops it after the run (or on error).
 */
void
gpsc_ExecutorRun_hook(QueryDesc *query_desc, ScanDirection direction,
					  uint64 count, bool execute_once)
{
	pg_qs_executor_run(query_desc);
	get_sender()->incr_depth();
	PG_TRY();
	{
		if (previous_ExecutorRun_hook)
			previous_ExecutorRun_hook(query_desc, direction, count,
									  execute_once);
		else
			standard_ExecutorRun(query_desc, direction, count, execute_once);
		get_sender()->decr_depth();
		pg_qs_pop_query();
	}
	PG_CATCH();
	{
		get_sender()->decr_depth();
		pg_qs_pop_query();
		PG_RE_THROW();
	}
	PG_END_TRY();
}

/*
 * gpsc_ExecutorFinish_hook -- wrapper for ExecutorFinish.
 *
 * Same push/pop pattern as ExecutorRun; keeps the QueryDesc visible to
 * signal handlers during the finish phase.
 */
void
gpsc_ExecutorFinish_hook(QueryDesc *query_desc)
{
	pg_qs_executor_finish(query_desc);
	get_sender()->incr_depth();
	PG_TRY();
	{
		if (previous_ExecutorFinish_hook)
			previous_ExecutorFinish_hook(query_desc);
		else
			standard_ExecutorFinish(query_desc);
		get_sender()->decr_depth();
		pg_qs_pop_query();
	}
	PG_CATCH();
	{
		get_sender()->decr_depth();
		pg_qs_pop_query();
		PG_RE_THROW();
	}
	PG_END_TRY();
}

/*
 * gpsc_ExecutorEnd_hook -- wrapper for ExecutorEnd.
 *
 * Notifies pg_query_state that the query is finishing (triggers the final
 * plan-tree walk and LOG dump), then calls the EventSender and the standard
 * end function.
 */
void
gpsc_ExecutorEnd_hook(QueryDesc *query_desc)
{
	pg_qs_executor_end(query_desc);
	cpp_call(get_sender(), &EventSender::executor_end, query_desc);
	if (previous_ExecutorEnd_hook)
		(*previous_ExecutorEnd_hook)(query_desc);
	else
		standard_ExecutorEnd(query_desc);
}

/*
 * gpsc_query_info_collect_hook -- wrapper for query_info_collect_hook.
 */
void
gpsc_query_info_collect_hook(QueryMetricsStatus status, void *arg)
{
	cpp_call(get_sender(), &EventSender::query_metrics_collect, status,
			 arg /* queryDesc */, false /* utility */, (ErrorData *) NULL);
	if (previous_query_info_collect_hook)
		(*previous_query_info_collect_hook)(status, arg);
}

#ifdef IC_TEARDOWN_HOOK
/*
 * gpsc_ic_teardown_hook -- wrapper for ic_teardown_hook.
 *
 * Collects interconnect metrics when the motion layer tears down.
 */
void
gpsc_ic_teardown_hook(ChunkTransportState *transportStates, bool hasErrors)
{
	cpp_call(get_sender(), &EventSender::ic_metrics_collect);
	if (previous_ic_teardown_hook)
		(*previous_ic_teardown_hook)(transportStates, hasErrors);
}
#endif

#ifdef ANALYZE_STATS_COLLECT_HOOK
/*
 * gpsc_analyze_stats_collect_hook -- wrapper for analyze_stats_collect_hook.
 */
void
gpsc_analyze_stats_collect_hook(QueryDesc *query_desc)
{
	cpp_call(get_sender(), &EventSender::analyze_stats_collect, query_desc);
	if (previous_analyze_stats_collect_hook)
		(*previous_analyze_stats_collect_hook)(query_desc);
}
#endif

/*
 * gpsc_process_utility_hook -- wrapper for ProcessUtility_hook.
 *
 * Constructs a minimal QueryDesc from the utility statement to reuse the
 * existing EventSender::query_metrics_collect interface.  The QueryDesc is
 * freed in both the success and error paths.
 */
static void
gpsc_process_utility_hook(PlannedStmt *pstmt, const char *queryString,
						  bool readOnlyTree, ProcessUtilityContext context,
						  ParamListInfo params, QueryEnvironment *queryEnv,
						  DestReceiver *dest, QueryCompletion *qc)
{
	QueryDesc *query_desc = (QueryDesc *) palloc0(sizeof(QueryDesc));
	query_desc->sourceText = queryString;

	cpp_call(get_sender(), &EventSender::query_metrics_collect,
			 METRICS_QUERY_SUBMIT, (void *) query_desc, true /* utility */,
			 (ErrorData *) NULL);

	get_sender()->incr_depth();
	PG_TRY();
	{
		if (previous_ProcessUtility_hook)
			(*previous_ProcessUtility_hook)(pstmt, queryString, readOnlyTree,
											context, params, queryEnv, dest,
											qc);
		else
			standard_ProcessUtility(pstmt, queryString, readOnlyTree, context,
									params, queryEnv, dest, qc);

		get_sender()->decr_depth();
		cpp_call(get_sender(), &EventSender::query_metrics_collect,
				 METRICS_QUERY_DONE, (void *) query_desc, true /* utility */,
				 (ErrorData *) NULL);
		pfree(query_desc);
	}
	PG_CATCH();
	{
		ErrorData     *edata;
		MemoryContext  oldctx;

		oldctx = MemoryContextSwitchTo(TopMemoryContext);
		edata  = CopyErrorData();
		FlushErrorState();
		MemoryContextSwitchTo(oldctx);

		get_sender()->decr_depth();
		cpp_call(get_sender(), &EventSender::query_metrics_collect,
				 METRICS_QUERY_ERROR, (void *) query_desc, true /* utility */,
				 edata);
		pfree(query_desc);
		ReThrowError(edata);
	}
	PG_END_TRY();
}

/*
 * check_stats_loaded -- raise ERROR if GpscStat shared memory is not mapped.
 *
 * Called by SQL-callable stat functions to guard against use before the
 * extension was loaded via shared_preload_libraries.
 */
static void
check_stats_loaded()
{
	if (!GpscStat::loaded())
		ereport(ERROR,
				(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
				 errmsg("gp_stats_collector must be loaded via "
						"shared_preload_libraries")));
}

/*
 * gpsc_functions_reset -- reset all GpscStat counters to zero.
 */
void
gpsc_functions_reset()
{
	check_stats_loaded();
	GpscStat::reset();
}

/*
 * gpsc_functions_get -- return the current GpscStat counters as a tuple.
 *
 * Returns one row with columns:
 *   segid, total_messages, send_failures, connection_failures,
 *   other_errors, max_message_size.
 */
Datum
gpsc_functions_get(FunctionCallInfo fcinfo)
{
	const int ATTNUM = 6;
	check_stats_loaded();
	auto stats = GpscStat::get_stats();
	TupleDesc tupdesc = CreateTemplateTupleDesc(ATTNUM);
	TupleDescInitEntry(tupdesc, (AttrNumber) 1, "segid", INT4OID, -1, 0);
	TupleDescInitEntry(tupdesc, (AttrNumber) 2, "total_messages", INT8OID, -1, 0);
	TupleDescInitEntry(tupdesc, (AttrNumber) 3, "send_failures", INT8OID, -1, 0);
	TupleDescInitEntry(tupdesc, (AttrNumber) 4, "connection_failures", INT8OID, -1, 0);
	TupleDescInitEntry(tupdesc, (AttrNumber) 5, "other_errors", INT8OID, -1, 0);
	TupleDescInitEntry(tupdesc, (AttrNumber) 6, "max_message_size", INT4OID, -1, 0);
	tupdesc = BlessTupleDesc(tupdesc);
	Datum values[ATTNUM];
	bool  nulls[ATTNUM];
	MemSet(nulls, 0, sizeof(nulls));
	values[0] = Int32GetDatum(GpIdentity.segindex);
	values[1] = Int64GetDatum(stats.total);
	values[2] = Int64GetDatum(stats.failed_sends);
	values[3] = Int64GetDatum(stats.failed_connects);
	values[4] = Int64GetDatum(stats.failed_other);
	values[5] = Int32GetDatum(stats.max_message_size);
	HeapTuple tuple = gpdb::heap_form_tuple(tupdesc, values, nulls);
	PG_RETURN_DATUM(HeapTupleGetDatum(tuple));
}

/*
 * test_uds_stop_server -- close and remove the test Unix-domain socket server.
 */
void
test_uds_stop_server()
{
	if (test_server_fd >= 0)
	{
		close(test_server_fd);
		test_server_fd = -1;
	}
	if (test_sock_path)
	{
		unlink(test_sock_path);
		pfree(test_sock_path);
		test_sock_path = NULL;
	}
}

/*
 * test_uds_start_server -- create a listening Unix-domain socket at path.
 *
 * Intended for integration tests that verify the UDS connector end-to-end.
 * Raises ERROR if the socket cannot be created or bound.
 */
void
test_uds_start_server(const char *path)
{
	struct sockaddr_un addr = {.sun_family = AF_UNIX};

	if (strlen(path) >= sizeof(addr.sun_path))
		ereport(ERROR, (errmsg("Unix socket path too long")));

	test_uds_stop_server();

	strlcpy(addr.sun_path, path, sizeof(addr.sun_path));
	test_sock_path = MemoryContextStrdup(TopMemoryContext, path);
	unlink(path);

	if ((test_server_fd = socket(AF_UNIX, SOCK_STREAM, 0)) < 0 ||
		bind(test_server_fd, (struct sockaddr *) &addr, sizeof(addr)) < 0 ||
		listen(test_server_fd, TEST_MAX_CONNECTIONS) < 0)
	{
		test_uds_stop_server();
		ereport(ERROR, (errmsg("test UDS socket setup failed: %m")));
	}
}

/*
 * test_uds_receive -- accept one connection and count bytes received.
 *
 * Polls for an incoming connection with a deadline of timeout_ms milliseconds.
 * Returns the total number of bytes received, or 0 on timeout.
 * Raises ERROR on poll/accept failures.
 */
int64
test_uds_receive(int timeout_ms)
{
	char          buf[TEST_RCV_BUF_SIZE];
	int           rc;
	struct pollfd pfd = {.fd = test_server_fd, .events = POLLIN};
	int64         total = 0;

	if (test_server_fd < 0)
		ereport(ERROR, (errmsg("test UDS server not started")));

	for (;;)
	{
		CHECK_FOR_INTERRUPTS();
		rc = poll(&pfd, 1, Min(timeout_ms, TEST_POLL_TIMEOUT_MS));
		if (rc > 0)
			break;
		if (rc < 0 && errno != EINTR)
			ereport(ERROR, (errmsg("poll: %m")));
		timeout_ms -= TEST_POLL_TIMEOUT_MS;
		if (timeout_ms <= 0)
			return total;
	}

	if (pfd.revents & POLLIN)
	{
		int     client = accept(test_server_fd, NULL, NULL);
		ssize_t n;

		if (client < 0)
			ereport(ERROR, (errmsg("accept: %m")));

		while ((n = recv(client, buf, sizeof(buf), 0)) != 0)
		{
			if (n > 0)
				total += n;
			else if (errno != EINTR)
				break;
		}
		close(client);
	}

	return total;
}
