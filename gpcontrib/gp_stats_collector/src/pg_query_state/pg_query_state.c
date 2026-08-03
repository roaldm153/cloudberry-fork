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
 * pg_query_state.c
 *		Core of the pg_query_state signal-dispatch layer.
 *
 * This module provides:
 *   - Shared-memory setup (shm_toc segment with params, mq, mq_req_id).
 *   - Custom ProcSignal registrations for three signals:
 *       QueryStatePollReason  -> SendQueryState()
 *       UserIdPollReason      -> SendCurrentUserId()
 *       BackendInfoPollReason -> SendCdbComponents()
 *   - GUC variables: pg_query_state.enable / enable_timing / enable_buffers.
 *   - Executor lifecycle hooks (start/run/finish/end) that maintain the
 *     QueryDescStack and enable instrumentation on the top-level query.
 *   - A requestor-side helper: shm_mq_receive_with_timeout().
 *
 * This is the "signal-only" variant: it does NOT push plan-node data to any
 * upstream sink (no UDS, no protobuf).  The executor_end hook merely walks
 * the plan tree and writes a LOG entry for debugging.
 *
 * Portions derived from pg_query_state
 * (https://github.com/postgrespro/pg_query_state), under the PostgreSQL
 * License:
 *   Portions Copyright (c) 2016-2025, Postgres Professional
 *
 * IDENTIFICATION
 *	  gpcontrib/gp_stats_collector/src/pg_query_state/pg_query_state.c
 *
 *-------------------------------------------------------------------------
 */

#include "pg_query_state.h"

#include "access/htup_details.h"
#include "access/xact.h"
#include "catalog/pg_type.h"
#include "cdb/cdbdispatchresult.h"
#include "cdb/cdbdisp_query.h"
#include "cdb/cdbexplain.h"
#include "cdb/cdbvars.h"
#include "executor/execParallel.h"
#include "executor/executor.h"
#include "fmgr.h"
#include "funcapi.h"
#include "miscadmin.h"
#include "nodes/nodeFuncs.h"
#include "nodes/print.h"
#include "parser/analyze.h"
#include "pgstat.h"
#include "postmaster/bgworker.h"
#include "storage/ipc.h"
#include "storage/s_lock.h"
#include "storage/spin.h"
#include "storage/procarray.h"
#include "storage/procsignal.h"
#include "storage/shm_toc.h"
#include "utils/guc.h"
#include "utils/timestamp.h"
#include "utils/lsyscache.h"
#include "utils/portal.h"
#include "utils/typcache.h"

#define TEXT_CSTR_CMP(text, cstr) \
	(memcmp(VARDATA(text), (cstr), VARSIZE(text) - VARHDRSZ))

/* GUC variables */
/* Master switch: disabling this suppresses all stat collection. */
bool pg_qs_enable  = true;

/* Collect timing (wall-clock) data in addition to row counts. */
bool pg_qs_timing  = false;

/* Collect buffer usage via Instrumentation.bufusage. */
bool pg_qs_buffers = true;

/*
 * Rolling counter incremented for every QueryDesc pushed onto the stack.
 * Used to generate synthetic queryId values for statements lacking one.
 */
static int qs_query_count = 0;

/* Saved hook pointer for chaining shmem_startup callbacks. */
static shmem_startup_hook_type prev_shmem_startup_hook = NULL;

/* Whether pg_qs_shmem_startup has completed successfully. */
static bool module_initialized = false;

/*
 * Monotonically increasing request counter on the requestor side.
 * Compared against *mq_req_id in the reply to detect stale responses.
 */
static int reqid = 0;

/* Shared-memory variables (pointers into the shm_toc segment) */
/* Table of contents anchoring the whole shared segment. */
static shm_toc *toc = NULL;

/*
 * Signal parameters written by the requestor and read by the handler.
 * Slot 0 in the toc.
 */
pg_qs_params *params = NULL;

/*
 * Raw shared memory queue used to return data from the handler.
 * Slot 1 in the toc.
 */
shm_mq *mq = NULL;

/*
 * Shared request-id counter.  The requestor increments it before sending a
 * signal; the handler echoes it back so the requestor can detect stale
 * replies.  Slot 2 in the toc.
 */
uint32 *mq_req_id = NULL;

/* Global signal-reason handles (set during pg_qs_init) */
List *QueryDescStack = NIL;

ProcSignalReason UserIdPollReason      = INVALID_PROCSIGNAL;
ProcSignalReason QueryStatePollReason  = INVALID_PROCSIGNAL;
ProcSignalReason BackendInfoPollReason = INVALID_PROCSIGNAL;

/* Forward declarations for module-private helpers */
static Size pg_qs_shmem_size(void);
static void pg_qs_shmem_startup(void);
static void push_query(QueryDesc *queryDesc);

static List *get_query_backend_info(ArrayType *array);

static shm_mq_result receive_msg_by_parts(shm_mq_handle *mqh, Size *total,
										  void **datap, int64 timeout,
										  int *rc, bool nowait);
static PG_QS_RequestResult GetRemoteBackendInfo(PGPROC *proc, List **result);
static void CollectQEQueryState(List *backendInfo);

#if PG_VERSION_NUM >= 150000
static shmem_request_hook_type prev_shmem_request_hook = NULL;
static void pg_qs_shmem_request(void);
#endif

/*
 * pg_qs_shmem_size -- compute the size of the shared memory segment.
 *
 * The segment holds three objects at fixed toc keys:
 *   key 0: pg_qs_params
 *   key 1: message queue of QUEUE_SIZE bytes
 *   key 2: uint32 request-id counter
 */
static Size
pg_qs_shmem_size(void)
{
	shm_toc_estimator e;
	Size size;
	int  nkeys = 3;

	shm_toc_initialize_estimator(&e);
	shm_toc_estimate_chunk(&e, sizeof(pg_qs_params));
	shm_toc_estimate_chunk(&e, (Size) QUEUE_SIZE);
	shm_toc_estimate_chunk(&e, sizeof(uint32));
	shm_toc_estimate_keys(&e, nkeys);
	size = shm_toc_estimate(&e);
	return size;
}

/*
 * pg_qs_shmem_startup -- attach to (or initialize) the shared segment.
 *
 * Called from the shmem_startup_hook chain after shared memory is mapped.
 * On first call (found == false) it initialises all sub-structures.
 * On subsequent calls it just re-attaches the toc pointers.
 */
static void
pg_qs_shmem_startup(void)
{
	bool  found;
	Size  shmem_size = pg_qs_shmem_size();
	void *shmem;
	int   num_toc = 0;

	LWLockAcquire(AddinShmemInitLock, LW_EXCLUSIVE);
	shmem = ShmemInitStruct("pg_query_state", shmem_size, &found);
	if (!found)
	{
		toc = shm_toc_create(PG_QS_MODULE_KEY, shmem, shmem_size);

		params = shm_toc_allocate(toc, sizeof(pg_qs_params));
		shm_toc_insert(toc, num_toc++, params);

		mq = shm_toc_allocate(toc, QUEUE_SIZE);
		shm_toc_insert(toc, num_toc++, mq);

		mq_req_id = shm_toc_allocate(toc, sizeof(uint32));
		shm_toc_insert(toc, num_toc++, mq_req_id);
		*mq_req_id = 0;
	}
	else
	{
		toc = shm_toc_attach(PG_QS_MODULE_KEY, shmem);
		params    = shm_toc_lookup(toc, num_toc++, false);
		mq        = shm_toc_lookup(toc, num_toc++, false);
		mq_req_id = shm_toc_lookup(toc, num_toc++, false);
	}
	LWLockRelease(AddinShmemInitLock);

	if (prev_shmem_startup_hook)
		prev_shmem_startup_hook();

	module_initialized = true;
}

#if PG_VERSION_NUM >= 150000
/*
 * pg_qs_shmem_request -- hook called to request shared memory space.
 *
 * PostgreSQL 15+ separates the request phase from the startup phase.
 * This hook is installed only when building against PG15+.
 */
static void
pg_qs_shmem_request(void)
{
	if (prev_shmem_request_hook)
		prev_shmem_request_hook();

	RequestAddinShmemSpace(pg_qs_shmem_size());
}
#endif

/*
 * pg_qs_init -- initialise the pg_query_state signal infrastructure.
 *
 * Must be called from _PG_init() while process_shared_preload_libraries_in_progress
 * is true.  Registers shared memory, custom ProcSignal handlers and GUC
 * variables.  Safe to call unconditionally for all roles.
 */
void
pg_qs_init(void)
{
	if (!process_shared_preload_libraries_in_progress)
		return;

#if PG_VERSION_NUM >= 150000
	prev_shmem_request_hook = shmem_request_hook;
	shmem_request_hook = pg_qs_shmem_request;
#else
	RequestAddinShmemSpace(pg_qs_shmem_size());
#endif

	UserIdPollReason      = RegisterCustomProcSignalHandler(SendCurrentUserId);
	QueryStatePollReason  = RegisterCustomProcSignalHandler(SendQueryState);
	BackendInfoPollReason = RegisterCustomProcSignalHandler(SendCdbComponents);

	if (QueryStatePollReason  == INVALID_PROCSIGNAL ||
		BackendInfoPollReason == INVALID_PROCSIGNAL ||
		UserIdPollReason      == INVALID_PROCSIGNAL)
	{
		ereport(WARNING, (errcode(ERRCODE_INSUFFICIENT_RESOURCES),
						  errmsg("pg_query_state isn't loaded: insufficient custom ProcSignal slots")));
		return;
	}

	DefineCustomBoolVariable("pg_query_state.enable",
							 "Enable module.",
							 NULL,
							 &pg_qs_enable,
							 true,
							 PGC_SUSET,
							 0,
							 NULL, NULL, NULL);

	DefineCustomBoolVariable("pg_query_state.enable_timing",
							 "Collect timing data, not just row counts.",
							 NULL,
							 &pg_qs_timing,
							 false,
							 PGC_SUSET,
							 0,
							 NULL, NULL, NULL);

	DefineCustomBoolVariable("pg_query_state.enable_buffers",
							 "Collect buffer usage.",
							 NULL,
							 &pg_qs_buffers,
							 true,
							 PGC_SUSET,
							 0,
							 NULL, NULL, NULL);

	prev_shmem_startup_hook = shmem_startup_hook;
	shmem_startup_hook = pg_qs_shmem_startup;

	elog(LOG, "pg_query_state: signal infrastructure initialised");
}

/* Executor lifecycle hooks */
/*
 * pg_qs_executor_start -- called at the start of executor execution.
 *
 * Enables instrumentation on the QueryDesc when:
 *   - pg_query_state is enabled
 *   - this is not an EXPLAIN-only execution
 *   - we are on a QD or QE role
 *   - there is no outer query already on the stack (top-level only)
 *   - the query passes the filter
 *   - no showstatctx is already attached
 *
 * Also assigns a synthetic queryId when the planner left it as zero.
 *
 * Parameters:
 *   queryDesc  -- the QueryDesc being started
 *   eflags     -- executor flags (EXEC_FLAG_EXPLAIN_ONLY etc.)
 */
void
pg_qs_executor_start(QueryDesc *queryDesc, int eflags)
{
	instr_time starttime;

	if (pg_qs_enable
		&& ((eflags & EXEC_FLAG_EXPLAIN_ONLY) == 0)
		&& (Gp_role == GP_ROLE_DISPATCH || Gp_role == GP_ROLE_EXECUTE)
		&& is_querystack_empty()
		&& filter_query(queryDesc)
		&& queryDesc->showstatctx == NULL)
	{
		queryDesc->instrument_options |= INSTRUMENT_CDB;
		queryDesc->instrument_options |= INSTRUMENT_ROWS;
		if (pg_qs_timing)
			queryDesc->instrument_options |= INSTRUMENT_TIMER;
		if (pg_qs_buffers)
			queryDesc->instrument_options |= INSTRUMENT_BUFFERS;

		INSTR_TIME_SET_CURRENT(starttime);
		queryDesc->showstatctx =
			cdbexplain_showExecStatsBegin(queryDesc, starttime);
		queryDesc->totaltime = InstrAlloc(1, INSTRUMENT_ALL, false);
	}

	if (queryDesc->plannedstmt->queryId == 0)
		queryDesc->plannedstmt->queryId =
			((uint64) gp_command_count << 32) + qs_query_count;
}

/*
 * pg_qs_executor_run -- called when the executor begins fetching tuples.
 *
 * Pushes the QueryDesc onto the stack so signal handlers can find it.
 */
void
pg_qs_executor_run(QueryDesc *queryDesc)
{
	push_query(queryDesc);
}

/*
 * pg_qs_executor_finish -- called after all tuples have been fetched.
 *
 * Pushes the QueryDesc again to keep the stack consistent during the finish
 * phase (needed so signal handlers still see the query during cleanup).
 */
void
pg_qs_executor_finish(QueryDesc *queryDesc)
{
	push_query(queryDesc);
}

/*
 * pg_qs_executor_end -- called when executor resources are released.
 *
 * Walks the plan tree with instrumentation finalized (InstrEndLoop) and writes
 * the collected per-node stats to the server log.
 */
void
pg_qs_executor_end(QueryDesc *queryDesc)
{
	QsWalkerContext *qs_walker_ctx;

	if (!queryDesc)
		return;

	qs_walker_ctx = (QsWalkerContext *) palloc0(sizeof(QsWalkerContext));
	qs_walker_ctx->finalize = true;
	qs_planstate_walker(queryDesc->planstate, qs_get_node_stats,
						qs_walker_ctx, 0);
	qs_debug_node_stats(qs_walker_ctx->per_node_stats);
}

/*
 * push_query -- add a QueryDesc to the top of the stack.
 *
 * Also increments qs_query_count for synthetic queryId generation.
 */
static void
push_query(QueryDesc *queryDesc)
{
	qs_query_count++;
	QueryDescStack = lcons(queryDesc, QueryDescStack);
}

/*
 * pg_qs_push_query -- public alias for push_query, called from hook_wrappers.
 */
void
pg_qs_push_query(QueryDesc *queryDesc)
{
	qs_query_count++;
	QueryDescStack = lcons(queryDesc, QueryDescStack);
}

/*
 * pg_qs_pop_query -- remove the most-recently-pushed QueryDesc from the stack.
 */
void
pg_qs_pop_query(void)
{
	QueryDescStack = list_delete_first(QueryDescStack);
}

/*
 * is_querystack_empty -- return true when no query is currently executing.
 */
bool
is_querystack_empty(void)
{
	return list_length(QueryDescStack) == 0;
}

/*
 * get_toppest_query -- return the most-recently-pushed QueryDesc, or NULL.
 */
QueryDesc *
get_toppest_query(void)
{
	return (QueryDescStack == NIL) ? NULL : (QueryDesc *) llast(QueryDescStack);
}

/*
 * filter_query -- decide whether to instrument a given QueryDesc.
 *
 * Returns false for cursor queries with non-default cursor options, and for
 * utility statements.  Returns true for SELECT, INSERT, UPDATE, DELETE.
 */
bool
filter_query(QueryDesc *queryDesc)
{
	Portal portal;

	if (queryDesc == NULL)
		return false;

	if (queryDesc->extended_query && queryDesc->portal_name)
	{
		portal = GetPortalByName(queryDesc->portal_name);
		if (portal->cursorOptions != CURSOR_OPT_NO_SCROLL)
			return false;
	}

	return (queryDesc->operation == CMD_SELECT  ||
			queryDesc->operation == CMD_DELETE  ||
			queryDesc->operation == CMD_INSERT  ||
			queryDesc->operation == CMD_UPDATE);
}

/*
 * wait_for_mq_detached -- spin until the caller has attached to the mq or
 * MAX_SND_TIMEOUT milliseconds elapses.
 *
 * Returns true if the queue was detached within the timeout (i.e. the other
 * end is done), false on timeout.
 */
bool
wait_for_mq_detached(shm_mq_handle *mqh)
{
	instr_time start_time;
	instr_time cur_time;
	int64 delay = MAX_SND_TIMEOUT;

	INSTR_TIME_SET_CURRENT(start_time);
	for (;;)
	{
		if (shm_mq_wait_for_attach(mqh) == SHM_MQ_DETACHED)
			break;
		WaitLatch(MyLatch,
				  WL_LATCH_SET | WL_EXIT_ON_PM_DEATH | WL_TIMEOUT,
				  delay, PG_WAIT_IPC);
		INSTR_TIME_SET_CURRENT(cur_time);
		INSTR_TIME_SUBTRACT(cur_time, start_time);
		delay = MAX_SND_TIMEOUT - (int64) INSTR_TIME_GET_MILLISEC(cur_time);
		if (delay <= 0)
		{
			elog(WARNING, "pg_query_state: wait_for_mq_detached timed out");
			return false;
		}
		CHECK_FOR_INTERRUPTS();
	}
	return true;
}

/*
 * LockShmem -- acquire an exclusive user-lock keyed by (PG_QS_MODULE_KEY, key).
 *
 * Used to serialise access to the shared mq between concurrent requestors
 * and between requestor and handler.
 */
void
LockShmem(LOCKTAG *tag, uint32 key)
{
	LockAcquireResult result;

	tag->locktag_field1 = PG_QS_MODULE_KEY;
	tag->locktag_field2 = key;
	tag->locktag_field3 = 0;
	tag->locktag_field4 = 0;
	tag->locktag_type   = LOCKTAG_USERLOCK;
	tag->locktag_lockmethodid = USER_LOCKMETHOD;

	result = LockAcquire(tag, ExclusiveLock, false, false);
	Assert(result == LOCKACQUIRE_OK);
}

/*
 * UnlockShmem -- release the exclusive user-lock acquired by LockShmem.
 */
void
UnlockShmem(LOCKTAG *tag)
{
	LockRelease(tag, ExclusiveLock, false);
}

/*
 * GetRemoteBackendInfo -- obtain the list of (segid, pid) pairs from QD.
 *
 * Sends BackendInfoPollReason to proc and waits for the reply.  On success,
 * *result is populated with gp_segment_pid entries (palloc'd).
 *
 * Returns the PG_QS_RequestResult code from the reply.
 */
static PG_QS_RequestResult
GetRemoteBackendInfo(PGPROC *proc, List **result)
{
	int sig_result;
	shm_mq_handle *mqh;
	shm_mq_result mq_receive_result;
	Size msg_len;
	backend_info *msg;
	LOCKTAG tag;
	int i;

	LockShmem(&tag, PG_QS_SND_KEY);
	params->reason = BackendInfoPollReason;
	mq = shm_mq_create(mq, QUEUE_SIZE);
	shm_mq_set_sender(mq, proc);
	shm_mq_set_receiver(mq, MyProc);
	*mq_req_id = reqid;
	UnlockShmem(&tag);

	sig_result = SendProcSignal(proc->pid, BackendInfoPollReason,
								proc->backendId);
	if (sig_result == -1)
		ereport(ERROR, (errcode(ERRCODE_INTERNAL_ERROR),
						errmsg("could not send BackendInfoPollReason signal")));

	mqh = shm_mq_attach(mq, NULL, NULL);
	mq_receive_result = shm_mq_receive_with_timeout(mqh, &msg_len,
													(void **) &msg,
													MAX_RCV_TIMEOUT);

	if (mq_receive_result != SHM_MQ_SUCCESS || msg == NULL ||
		msg->reqid != (uint32) reqid)
	{
		shm_mq_detach(mqh);
		ereport(WARNING, (errcode(ERRCODE_INTERNAL_ERROR),
						  errmsg("GetRemoteBackendInfo: message not received")));
		return QUERY_NOT_RUNNING;
	}

	if (msg->result_code != QS_RETURNED)
	{
		PG_QS_RequestResult result_code = msg->result_code;
		shm_mq_detach(mqh);
		return result_code;
	}

	{
		int expected_len = BASE_SIZEOF_GP_BACKEND_INFO +
						   msg->number * sizeof(gp_segment_pid);
		if ((int) msg_len != expected_len)
		{
			shm_mq_detach(mqh);
			ereport(ERROR, (errcode(ERRCODE_INTERNAL_ERROR),
							errmsg("GetRemoteBackendInfo: unexpected message length")));
		}
	}

	for (i = 0; i < msg->number; i++)
	{
		gp_segment_pid *segpid = palloc(sizeof(gp_segment_pid));
		*segpid = msg->pids[i];
		*result = lcons(segpid, *result);
	}

	shm_mq_detach(mqh);
	return QS_RETURNED;
}

/*
 * CollectQEQueryState -- fan-out query-state signals to all QE backends.
 *
 * Dispatches a cbdb_mpp_query_state() call to each segment listed in
 * backendInfo.  Results are returned as raw CdbPgResults.
 */
static void
CollectQEQueryState(List *backendInfo)
{
	ListCell       *lc;
	int             index = 0;
	StringInfoData  params_buf;
	char           *sql;

	if (list_length(backendInfo) == 0)
		return;

	initStringInfo(&params_buf);

	foreach(lc, backendInfo)
	{
		gp_segment_pid *segpid = (gp_segment_pid *) lfirst(lc);
		index++;
		appendStringInfo(&params_buf, "'(%d,%d)'", segpid->segid, segpid->pid);
		if (index != list_length(backendInfo))
			appendStringInfoChar(&params_buf, ',');
	}

	sql = psprintf("SELECT gpsc.cbdb_mpp_query_state((ARRAY[%s])::gpsc.gp_segment_pid[])",
				   params_buf.data);

	CdbDispatchCommand(sql, DF_NONE, NULL);
	pfree(params_buf.data);
	pfree(sql);
}

/*
 * shm_mq_receive_with_timeout -- receive from mqh, blocking up to `timeout` ms.
 *
 * Calls receive_msg_by_parts() in a loop, sleeping on the latch between
 * retries.  Returns SHM_MQ_SUCCESS, SHM_MQ_DETACHED, or SHM_MQ_WOULD_BLOCK
 * (the last meaning the timeout expired).
 *
 * On success, *nbytesp is set to the message length and *datap to a palloc'd
 * buffer containing the message.
 */
shm_mq_result
shm_mq_receive_with_timeout(shm_mq_handle *mqh,
							Size *nbytesp,
							void **datap,
							int64 timeout)
{
	int        rc = 0;
	int64      delay = timeout;
	instr_time start_time;
	instr_time cur_time;

	INSTR_TIME_SET_CURRENT(start_time);

	for (;;)
	{
		shm_mq_result result;

		result = receive_msg_by_parts(mqh, nbytesp, datap, timeout, &rc, true);
		if (result != SHM_MQ_WOULD_BLOCK)
			return result;

		if (rc & WL_TIMEOUT || delay <= 0)
			return SHM_MQ_WOULD_BLOCK;

		rc = WaitLatch(MyLatch,
					   WL_LATCH_SET | WL_EXIT_ON_PM_DEATH | WL_TIMEOUT,
					   delay, PG_WAIT_EXTENSION);

		INSTR_TIME_SET_CURRENT(cur_time);
		INSTR_TIME_SUBTRACT(cur_time, start_time);
		delay = timeout - (int64) INSTR_TIME_GET_MILLISEC(cur_time);
		if (delay <= 0)
			return SHM_MQ_WOULD_BLOCK;

		CHECK_FOR_INTERRUPTS();
		ResetLatch(MyLatch);
	}
}

/*
 * receive_msg_by_parts -- reassemble a multi-chunk message from mqh.
 *
 * The wire protocol prefixes each message with its total byte count (a Size),
 * followed by one or more chunks of up to MSG_MAX_SIZE bytes.  This function
 * reads the prefix, allocates a buffer, and loops until all chunks arrive.
 *
 * Parameters:
 *   mqh     -- attached message-queue handle
 *   total   -- out: total bytes received
 *   datap   -- out: palloc'd buffer with reassembled message
 *   timeout -- caller's deadline in ms (used only for PART_RCV_DELAY retries)
 *   rc      -- out: WaitLatch flags (set to WL_TIMEOUT if we give up)
 *   nowait  -- passed through to shm_mq_receive
 */
static shm_mq_result
receive_msg_by_parts(shm_mq_handle *mqh, Size *total, void **datap,
					 int64 timeout, int *rc, bool nowait)
{
	shm_mq_result  mq_receive_result;
	shm_mq_msg    *buff;
	int            offset;
	Size          *expected;
	Size           expected_data;
	Size           len;

	/* Read the length prefix. */
	mq_receive_result = shm_mq_receive(mqh, &len, (void **) &expected, nowait);
	if (mq_receive_result != SHM_MQ_SUCCESS)
		return mq_receive_result;
	Assert(len == sizeof(Size));

	expected_data = *expected;
	*datap = palloc0(expected_data);

	/* Reassemble chunks until we have expected_data bytes. */
	for (offset = 0; offset < (int) expected_data; )
	{
		int64 delay = timeout;

		for (;;)
		{
			mq_receive_result = shm_mq_receive(mqh, &len, (void **) &buff,
											   nowait);
			if (mq_receive_result != SHM_MQ_SUCCESS)
			{
				if (nowait && mq_receive_result == SHM_MQ_WOULD_BLOCK)
				{
					if (delay > 0)
					{
						pg_usleep(PART_RCV_DELAY * 1000);
						delay -= PART_RCV_DELAY;
						continue;
					}
					if (rc)
						*rc |= WL_TIMEOUT;
				}
				return mq_receive_result;
			}
			break;
		}
		memcpy((char *) *datap + offset, buff, len);
		offset += len;
	}

	*total = offset;
	return mq_receive_result;
}

/* SQL callable functions */
/*
 * pg_query_state -- entry point for the pg_query_state() SQL function.
 *
 * Obtains the user-id and segment-backend list from the target backend,
 * then fans out cbdb_mpp_query_state() to each QE.
 */
PG_FUNCTION_INFO_V1(pg_query_state);
Datum
pg_query_state(PG_FUNCTION_ARGS)
{
	pid_t                pid = PG_GETARG_INT32(0);
	PGPROC              *proc;
	LOCKTAG              tag;
	PG_QS_RequestResult  result;
	List                *backend_info = NIL;
	Oid					 counterpart_user_id;

	if (pid == MyProcPid)
		ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
						errmsg("cannot extract state of current process")));

	if (!module_initialized)
		ereport(ERROR,
				(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
				 errmsg("pg_query_state must be loaded via shared_preload_libraries")));

	proc = BackendPidGetProc(pid);
	if (!proc || proc->backendId == InvalidBackendId ||
		proc->databaseId == InvalidOid || proc->roleId == InvalidOid)
		ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
						errmsg("backend with pid=%d not found", pid)));

	counterpart_user_id = proc->roleId;
	if (!(superuser() || GetUserId() == counterpart_user_id))
	{
		ereport(ERROR, (errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
						errmsg("permission denied")));
	}

	LockShmem(&tag, PG_QS_RCV_KEY);
	reqid = *mq_req_id + 1;
	result = GetRemoteBackendInfo(proc, &backend_info);
	UnlockShmem(&tag);

	switch (result)
	{
		case QUERY_NOT_RUNNING:
			elog(DEBUG1, "pg_query_state: pid=%d is not running a query", pid);
			break;

		case STAT_DISABLED:
			elog(DEBUG1, "pg_query_state: stats collection disabled");
			break;

		case QS_RETURNED:
			/* Signal all segment QEs to push their plan-node stats via UDS. */
			CollectQEQueryState(backend_info);

			/*
			 * Signal the QD backend itself so it pushes coordinator-side plan
			 * nodes and the plan-doc.  SendQueryState() emits directly via UDS.
			 */
			SendProcSignal(proc->pid, QueryStatePollReason, proc->backendId);
			break;
	}

	PG_RETURN_VOID();
}

/*
 * pg_query_state_backends -- list the QE backends participating in the query
 * running on backend `pid`.
 *
 * Returns a set of (segid, pid) rows obtained from the coordinator via
 * GetRemoteBackendInfo (the same list the poll path fans out to).  A consumer
 * can use the row count as the expected number of backends that will report.
 *
 * Uses the materialize SRF mode: the whole list is built into a tuplestore in
 * one call.  Returns an empty set when the target query is not running.
 */
PG_FUNCTION_INFO_V1(pg_query_state_backends);
Datum
pg_query_state_backends(PG_FUNCTION_ARGS)
{
	pid_t                pid = PG_GETARG_INT32(0);
	ReturnSetInfo       *rsinfo = (ReturnSetInfo *) fcinfo->resultinfo;
	TupleDesc            tupdesc;
	Tuplestorestate     *tupstore;
	MemoryContext        per_query_ctx;
	MemoryContext        oldcontext;
	PGPROC              *proc;
	List                *backend_info = NIL;
	LOCKTAG              tag;
	PG_QS_RequestResult  info_result;
	ListCell            *lc;
	Oid					 counterpart_user_id;

	/* Standard set-returning-function materialize-mode preamble. */
	if (rsinfo == NULL || !IsA(rsinfo, ReturnSetInfo))
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("set-valued function called in context that cannot accept a set")));
	if (!(rsinfo->allowedModes & SFRM_Materialize))
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("materialize mode required, but it is not allowed in this context")));
	if (get_call_result_type(fcinfo, NULL, &tupdesc) != TYPEFUNC_COMPOSITE)
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("function returning record called in context that cannot accept type record")));

	per_query_ctx = rsinfo->econtext->ecxt_per_query_memory;
	oldcontext = MemoryContextSwitchTo(per_query_ctx);
	tupstore = tuplestore_begin_heap(true, false, work_mem);
	rsinfo->returnMode = SFRM_Materialize;
	rsinfo->setResult = tupstore;
	rsinfo->setDesc = tupdesc;
	MemoryContextSwitchTo(oldcontext);

	if (pid == MyProcPid)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("cannot extract state of current process")));

	proc = BackendPidGetProc(pid);
	if (!proc || proc->backendId == InvalidBackendId ||
		proc->databaseId == InvalidOid || proc->roleId == InvalidOid)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("backend with pid=%d not found", pid)));

	counterpart_user_id = proc->roleId;
	if (!(superuser() || GetUserId() == counterpart_user_id))
	{
		ereport(ERROR, (errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
						errmsg("permission denied")));
	}

	if (!module_initialized)
	{
		UnlockShmem(&tag);
		ereport(ERROR,
				(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
				 errmsg("pg_query_state must be loaded via shared_preload_libraries")));
	}

	LockShmem(&tag, PG_QS_RCV_KEY);
	reqid = *mq_req_id + 1;
	info_result = GetRemoteBackendInfo(proc, &backend_info);
	UnlockShmem(&tag);

	/* Not running / disabled: return an empty set rather than erroring. */
	if (info_result != QS_RETURNED)
		return (Datum) 0;

	foreach(lc, backend_info)
	{
		gp_segment_pid *segpid = (gp_segment_pid *) lfirst(lc);
		Datum           values[2];
		bool            nulls[2] = {false, false};

		values[0] = Int32GetDatum(segpid->segid);
		values[1] = Int32GetDatum(segpid->pid);
		tuplestore_putvalues(tupstore, tupdesc, values, nulls);
	}

	return (Datum) 0;
}

/*
 * cbdb_mpp_query_state -- QE-side entry point dispatched by CollectQEQueryState.
 *
 * Receives an array of gp_segment_pid, filters those belonging to this
 * segment, and fires QueryStatePollReason at each matching backend.
 */
PG_FUNCTION_INFO_V1(cbdb_mpp_query_state);
Datum
cbdb_mpp_query_state(PG_FUNCTION_ARGS)
{
	ListCell *iter;
	List     *alive_procs = get_query_backend_info(PG_GETARG_ARRAYTYPE_P(0));

	if (alive_procs == NIL)
		PG_RETURN_NULL();

	/* Set request parameters for signal handler. */
	params->verbose  = true;
	params->costs    = true;
	params->timing   = true;
	params->buffers  = true;
	params->triggers = false;
	params->format   = EXPLAIN_FORMAT_JSON;

	foreach(iter, alive_procs)
	{
		PGPROC *proc = (PGPROC *) lfirst(iter);
		int sig_result;

		if (!proc)
			continue;

		sig_result = SendProcSignal(proc->pid, QueryStatePollReason,
									proc->backendId);
		if (sig_result == -1)
			ereport(ERROR, (errcode(ERRCODE_INTERNAL_ERROR),
							errmsg("cbdb_mpp_query_state: failed to send signal to pid %d",
								   proc->pid)));
	}
	PG_RETURN_VOID();
}

/*
 * get_query_backend_info -- convert a gp_segment_pid[] SQL array to a list
 * of PGPROC pointers for backends running on this segment.
 *
 * Skips entries for other segments and entries whose backend has exited.
 */
static List *
get_query_backend_info(ArrayType *array)
{
	int16  typlen;
	bool   typbyval;
	char   typalign;
	Oid    element_type = ARR_ELEMTYPE(array);
	Datum *data;
	bool  *nulls;
	int    nitems;
	int    len;
	List  *alive_procs = NIL;

	get_typlenbyvalalign(element_type, &typlen, &typbyval, &typalign);
	deconstruct_array(array, element_type, typlen, typbyval, typalign,
					  &data, &nulls, &nitems);

	len = ArrayGetNItems(ARR_NDIM(array), ARR_DIMS(array));

	for (int i = 0; i < len; i++)
	{
		HeapTupleHeader td = DatumGetHeapTupleHeader(data[i]);
		TupleDesc       tupDesc;
		HeapTupleData   tmptup;
		int32           pid;
		int32           segid;
		bool            isnull = false;
		PGPROC         *proc;

		tupDesc = lookup_rowtype_tupdesc_copy(
			HeapTupleHeaderGetTypeId(td), HeapTupleHeaderGetTypMod(td));
		tmptup.t_len  = HeapTupleHeaderGetDatumLength(td);
		tmptup.t_data = td;

		segid = DatumGetInt32(heap_getattr(&tmptup, 1, tupDesc, &isnull));
		if (isnull || segid != GpIdentity.segindex)
			continue;

		pid = DatumGetInt32(heap_getattr(&tmptup, 2, tupDesc, &isnull));
		if (isnull)
			continue;

		proc = BackendPidGetProc(pid);
		if (proc == NULL)
			continue;

		alive_procs = lappend(alive_procs, proc);
	}
	return alive_procs;
}
