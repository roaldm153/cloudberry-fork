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
 * pg_query_state.h
 *		Public API for the pg_query_state signal-dispatch layer.
 *
 * This header is included by both the C extension entry point
 * (gp_stats_collector.c) and the C++ hook wrappers (hook_wrappers.cpp).
 * Keep it C-compatible: no C++ types, wrapped in extern "C".
 *
 * Portions derived from pg_query_state
 * (https://github.com/postgrespro/pg_query_state), under the PostgreSQL
 * License:
 *   Portions Copyright (c) 2016-2025, Postgres Professional
 *
 * IDENTIFICATION
 *	  gpcontrib/gp_stats_collector/src/pg_query_state/pg_query_state.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef __PG_QUERY_STATE_H__
#define __PG_QUERY_STATE_H__
#ifdef __cplusplus
extern "C" {
#endif

#include "postgres.h"

#include "commands/explain.h"
#include "nodes/pg_list.h"
#include "storage/procarray.h"
#include "storage/shm_mq.h"
#include "cdb/cdbdispatchresult.h"
#include "qs_types.h"

/* Shared memory queue capacity for passing query-state messages. */
#define QUEUE_SIZE          (64 * 1024)

/* Maximum single chunk size when splitting a message across shm_mq sends. */
#define MSG_MAX_SIZE        (4 * 1024)

/* Delay between shm_mq send retries, in microseconds (100 ms). */
#define WRITING_DELAY       (100 * 1000)

/* Maximum number of send retries before giving up. */
#define NUM_OF_ATTEMPTS     6

/* Bitmask flags for caller-side warnings embedded in shm_mq_msg.warnings. */
#define TIMINIG_OFF_WARNING 1
#define BUFFERS_OFF_WARNING 2

/* Unique key that identifies our shm_toc segment. */
#define PG_QS_MODULE_KEY    0xCA94B108

/* Table-of-contents slot indices within the shm_toc segment. */
#define PG_QS_RCV_KEY       0
#define PG_QS_SND_KEY       1

/*
 * Timeouts for shm_mq operations.
 * The receive timeout must exceed the send timeout so that waiting workers
 * always give up before the polling process stops listening.
 */
#define MAX_RCV_TIMEOUT     2000  /* ms */
#define MAX_SND_TIMEOUT     1000  /* ms */

/*
 * Sleep between partial-receive retries (SHM_MQ_WOULD_BLOCK case).
 * Must be less than MAX_RCV_TIMEOUT.
 */
#define PART_RCV_DELAY      100   /* ms */

/*
 * Minimum interval between coordinator plan-doc pushes for the same query.
 * SendQueryState() re-sends the ExplainPrintPlan document only after this
 * interval elapses, so repeated polls of a long-running query do not resend
 * the (unchanging) plan on every signal.
 */
#define PLAN_DOC_RESEND_INTERVAL_MS (2 * 60 * 1000)

/*
 * Status codes returned by the signal handler to describe the state of the
 * queried backend.
 */
typedef enum
{
	QUERY_NOT_RUNNING,   /* backend is idle or has no active QueryDesc */
	STAT_DISABLED,       /* pg_query_state.enable = false */
	QS_RETURNED          /* handler successfully collected and sent stats */
} PG_QS_RequestResult;

/*
 * Wire format for a query-state reply message transmitted through shm_mq.
 * The variable-length `stack` field carries sequentially laid out text frames,
 * one per stack depth.
 */
typedef struct
{
	int reqid;
	int length;           /* total message size including flexible array */
	PGPROC *proc;
	PG_QS_RequestResult result_code;
	int warnings;         /* bitmask of TIMINIG_OFF_WARNING / BUFFERS_OFF_WARNING */
	int stack_depth;
	char stack[FLEXIBLE_ARRAY_MEMBER];
} shm_mq_msg;

/*
 * Wire format for the user-id polling reply.
 */
typedef struct
{
	Oid    userid;
	uint32 reqid;
} shm_mq_userid_msg;

#define BASE_SIZEOF_SHM_MQ_MSG (offsetof(shm_mq_msg, stack_depth))

/*
 * Compact identifier for a backend running on a specific segment.
 */
typedef struct
{
	int32 segid;
	int32 pid;
} gp_segment_pid;

/*
 * Wire format for the backend-info (CDB segment PIDs) reply.
 */
typedef struct
{
	int reqid;
	int length;
	PGPROC *proc;
	PG_QS_RequestResult result_code;
	int number;
	gp_segment_pid pids[FLEXIBLE_ARRAY_MEMBER];
} backend_info;

#define BASE_SIZEOF_GP_BACKEND_INFO (offsetof(backend_info, pids))

/*
 * Parameters passed through shared memory from the requestor to the signal
 * handler, controlling what the handler should collect and how.
 */
typedef struct
{
	ProcSignalReason reason;
	int     reqid;
	bool    verbose;
	bool    costs;
	bool    timing;
	bool    buffers;
	bool    triggers;
	ExplainFormat format;
} pg_qs_params;

/*
 * Context threaded through the plan-tree walker.
 * per_node_stats accumulates one GpscNodeSample per visited node.
 */
typedef struct QsWalkerContext
{
	List    *per_node_stats;
	int32_t  parent_plan_node_id;
	bool 	 finalize; /* true only in pg_qs_executor end */
	TimestampTz ts_now;
} QsWalkerContext;

/*
 * Result code for the chunked shm_mq send helper.
 */
typedef enum
{
	MSG_BY_PARTS_SUCCEEDED,
	MSG_BY_PARTS_FAILED
} msg_by_parts_result;

extern bool           pg_qs_enable;
extern bool           pg_qs_timing;
extern bool           pg_qs_buffers;
extern List          *QueryDescStack;
extern pg_qs_params  *params;
extern shm_mq        *mq;
extern uint32        *mq_req_id;

/*
 * Per-backend trace_id slots, indexed by BackendId (1..MaxBackends; slot 0 for
 * InvalidBackendId is unused).  The single shared `params` cannot carry the
 * trace across an asynchronous ProcSignal: two concurrent collections would
 * clobber it and a signaled backend would stamp its batch with the wrong
 * trace.  The dispatcher writes qs_trace_slots[target->backendId] before
 * signalling; the signaled backend reads qs_trace_slots[MyBackendId].  The slot
 * is keyed by backend, not by collection, so two overlapping collections of the
 * same backend still share one slot -- the caller must not poll one pid twice
 * concurrently.
 */
extern char (*qs_trace_slots)[GPSC_TRACE_ID_LEN];

extern ProcSignalReason UserIdPollReason;
extern ProcSignalReason QueryStatePollReason;
extern ProcSignalReason BackendInfoPollReason;

/*
 * pg_qs_init -- register shared memory, custom signals and GUC variables.
 * Must be called from _PG_init() during shared_preload_libraries processing.
 */
extern void pg_qs_init(void);

/* Executor lifecycle hooks -- called from hook_wrappers.cpp. */
extern void pg_qs_executor_start(QueryDesc *queryDesc, int eflags);
extern void pg_qs_executor_run(QueryDesc *queryDesc);
extern void pg_qs_executor_finish(QueryDesc *queryDesc);
extern void pg_qs_executor_end(QueryDesc *queryDesc);

/* Shared-memory queue receive helper with millisecond deadline. */
extern shm_mq_result shm_mq_receive_with_timeout(shm_mq_handle *mqh,
												  Size *nbytesp,
												  void **datap,
												  int64 timeout);

/* QueryDescStack push/pop helpers. */
extern void pg_qs_pop_query(void);
extern void pg_qs_push_query(QueryDesc *);

/* Custom signal handlers registered with RegisterCustomProcSignalHandler. */
extern void SendQueryState(void);
extern void SendCurrentUserId(void);
extern void SendCdbComponents(void);

/* Shared-memory lock helpers. */
extern void UnlockShmem(LOCKTAG *tag);
extern void LockShmem(LOCKTAG *tag, uint32 key);

/* Chunked shm_mq send. */
extern msg_by_parts_result send_msg_by_parts(shm_mq_handle *mqh,
											 Size nbytes,
											 const void *data);

/* Plan-tree walker and per-node stat collectors. */
typedef void (*qs_planstate_walker_callback)(PlanState *, QsWalkerContext *);
extern void qs_planstate_walker(PlanState *, qs_planstate_walker_callback,
								QsWalkerContext *, int depth);
extern void qs_get_node_stats(PlanState *, QsWalkerContext *);

/* Debug logging helpers -- emit collected stats to PostgreSQL LOG. */
extern void qs_debug_node_stats(List *per_node_stats);
extern void qs_debug_node_sample(GpscNodeSample *sample);

/*
 * emit_node_batch -- flatten a List<GpscNodeSample *> into an array and push
 * it to the yagpcc UDS sink as a single SetPerNodeBatchReq (one connection
 * per backend).  No-op on an empty list.  The caller must have invoked
 * gpsc_qs_sync_config() first.
 */
extern void emit_node_batch(List *per_node_stats, const char *trace_id);

/* Query filtering and miscellaneous helpers. */
extern bool filter_query(QueryDesc *queryDesc);
extern bool wait_for_mq_detached(shm_mq_handle *mqh);
extern bool is_querystack_empty(void);
extern QueryDesc *get_toppest_query(void);

extern void gpsc_reset_node_roll_state(void);

#ifdef __cplusplus
}
#endif
#endif /* __PG_QUERY_STATE_H__ */
