/*
 * signal_handler.c
 *		Custom signal handlers and plan-tree walker for pg_query_state.
 *
 * This module implements the three custom ProcSignal handlers registered by
 * pg_qs_init():
 *
 *   SendQueryState()    -- fired when QueryStatePollReason is received.
 *                          Walks the active plan tree, collects per-node stats,
 *                          and writes them to the PostgreSQL LOG.
 *   SendCurrentUserId() -- fired when UserIdPollReason is received.
 *                          Sends the current effective user-id through shm_mq.
 *   SendCdbComponents() -- fired when BackendInfoPollReason is received (QD only).
 *                          Sends the list of active QE (segid, pid) pairs.
 *
 * Also contains:
 *   qs_planstate_walker()   -- recursive plan-tree traversal helper.
 *   qs_get_node_stats()     -- per-node stat collection callback.
 *   qs_debug_node_stats()   -- LOG-level dump of a collected stat list.
 *   qs_debug_node_sample()  -- LOG-level dump of a single GpscNodeSample.
 *   send_msg_by_parts()     -- chunked shm_mq send helper.
 *
 * Copyright (c) 2016-2024, Postgres Professional
 *
 * IDENTIFICATION
 *	  gpcontrib/gp_stats_collector/src/pg_query_state/signal_handler.c
 */

#include <unistd.h>

#include "pg_query_state.h"

#include "cdb/cdbexplain.h"
#include "cdb/cdbutil.h"
#include "cdb/cdbvars.h"
#include "libpq-fe.h"
#include "cdb/cdbconn.h"
#include "commands/explain.h"
#include "miscadmin.h"
#include "pgstat.h"
#include "storage/bufmgr.h"
#include "storage/lock.h"
#include "utils/builtins.h"
#include "utils/memutils.h"
#include "libpq/pqmq.h"

/*
 * shm_mq_send_nonblocking -- attempt to send nbytes through mqh up to
 * `attempts` times, sleeping WRITING_DELAY µs between retries.
 *
 * Returns MSG_BY_PARTS_FAILED immediately on SHM_MQ_DETACHED; retries on
 * SHM_MQ_WOULD_BLOCK.
 */
static msg_by_parts_result
shm_mq_send_nonblocking(shm_mq_handle *mqh, Size nbytes,
						const void *data, Size attempts)
{
	int i;
	shm_mq_result res;

	for (i = 0; i < (int) attempts; i++)
	{
#if PG_VERSION_NUM < 150000
		res = shm_mq_send(mqh, nbytes, data, true);
#else
		res = shm_mq_send(mqh, nbytes, data, true, true);
#endif

		if (res == SHM_MQ_SUCCESS)
			break;
		else if (res == SHM_MQ_DETACHED)
			return MSG_BY_PARTS_FAILED;

		/* SHM_MQ_WOULD_BLOCK -- back off briefly and retry. */
		pg_usleep(WRITING_DELAY);
	}

	if (i == (int) attempts)
		return MSG_BY_PARTS_FAILED;

	return MSG_BY_PARTS_SUCCEEDED;
}

/*
 * send_msg_by_parts -- transmit an arbitrarily large buffer through mqh.
 *
 * The wire protocol is: first send a Size value announcing the total payload
 * length, then send the payload itself in chunks of at most MSG_MAX_SIZE
 * bytes.  The receiver must use receive_msg_by_parts() (in pg_query_state.c)
 * to reassemble the chunks.
 *
 * Parameters:
 *   mqh    -- attached shm_mq handle (sender side)
 *   nbytes -- total payload size
 *   data   -- pointer to the payload
 *
 * Returns MSG_BY_PARTS_SUCCEEDED on success, MSG_BY_PARTS_FAILED otherwise.
 */
msg_by_parts_result
send_msg_by_parts(shm_mq_handle *mqh, Size nbytes, const void *data)
{
	int offset;
	int bytes_left;
	int bytes_send;

	/* Announce total length. */
	if (shm_mq_send_nonblocking(mqh, sizeof(Size), &nbytes,
								NUM_OF_ATTEMPTS) == MSG_BY_PARTS_FAILED)
		return MSG_BY_PARTS_FAILED;

	/* Send payload in chunks. */
	for (offset = 0; offset < (int) nbytes; offset += bytes_send)
	{
		bytes_left = nbytes - offset;
		bytes_send = (bytes_left < MSG_MAX_SIZE) ? bytes_left : MSG_MAX_SIZE;
		if (shm_mq_send_nonblocking(mqh, bytes_send,
									&(((unsigned char *) data)[offset]),
									NUM_OF_ATTEMPTS) == MSG_BY_PARTS_FAILED)
			return MSG_BY_PARTS_FAILED;
	}

	return MSG_BY_PARTS_SUCCEEDED;
}

/*
 * qs_planstate_walker -- depth-first traversal of a PlanState tree.
 *
 * Visits every node in the tree rooted at `planstate`, calling `executor`
 * on each node before recursing.  Handles all node types that have child
 * plan states (Append, MergeAppend, BitmapAnd/Or, SubqueryScan, CustomScan,
 * init-plans, and sub-plans).
 *
 * Parameters:
 *   planstate     -- root of the subtree to walk (NULL is a no-op)
 *   executor      -- callback invoked for each node
 *   qs_walker_ctx -- context threaded through all callbacks
 *   depth         -- current recursion depth (for stack-depth checks)
 */
void
qs_planstate_walker(PlanState *planstate,
					qs_planstate_walker_callback executor,
					QsWalkerContext *qs_walker_ctx,
					int depth)
{
	int32     saved_parent_plan_node_id;
	Plan     *plan;
	ListCell *lc;

	if (planstate == NULL)
		return;

	check_stack_depth();

	plan = planstate->plan;

	executor(planstate, qs_walker_ctx);
	saved_parent_plan_node_id = qs_walker_ctx->parent_plan_node_id;
	qs_walker_ctx->parent_plan_node_id = plan->plan_node_id;

	/* initPlans */
	foreach(lc, planstate->initPlan)
	{
		SubPlanState *sps = lfirst_node(SubPlanState, lc);
		qs_planstate_walker(sps->planstate, executor, qs_walker_ctx, depth + 1);
	}

	/* Left and right children. */
	qs_planstate_walker(outerPlanState(planstate), executor, qs_walker_ctx,
						depth + 1);
	qs_planstate_walker(innerPlanState(planstate), executor, qs_walker_ctx,
						depth + 1);

	/* Type-specific child plans. */
	switch (nodeTag(plan))
	{
		case T_Append:
		{
			AppendState *as = (AppendState *) planstate;
			for (int i = 0; i < as->as_nplans; i++)
				qs_planstate_walker(as->appendplans[i], executor,
									qs_walker_ctx, depth + 1);
			break;
		}
		case T_MergeAppend:
		{
			MergeAppendState *ms = (MergeAppendState *) planstate;
			for (int i = 0; i < ms->ms_nplans; i++)
				qs_planstate_walker(ms->mergeplans[i], executor,
									qs_walker_ctx, depth + 1);
			break;
		}
		case T_BitmapAnd:
		{
			BitmapAndState *bas = (BitmapAndState *) planstate;
			for (int i = 0; i < bas->nplans; i++)
				qs_planstate_walker(bas->bitmapplans[i], executor,
									qs_walker_ctx, depth + 1);
			break;
		}
		case T_BitmapOr:
		{
			BitmapOrState *bos = (BitmapOrState *) planstate;
			for (int i = 0; i < bos->nplans; i++)
				qs_planstate_walker(bos->bitmapplans[i], executor,
									qs_walker_ctx, depth + 1);
			break;
		}
		case T_SubqueryScan:
			qs_planstate_walker(((SubqueryScanState *) planstate)->subplan,
								executor, qs_walker_ctx, depth + 1);
			break;
		case T_CustomScan:
			foreach(lc, ((CustomScanState *) planstate)->custom_ps)
				qs_planstate_walker((PlanState *) lfirst(lc), executor,
									qs_walker_ctx, depth + 1);
			break;
		default:
			break;
	}

	/* subPlans */
	foreach(lc, planstate->subPlan)
	{
		SubPlanState *sps = lfirst_node(SubPlanState, lc);
		qs_planstate_walker(sps->planstate, executor, qs_walker_ctx, depth + 1);
	}

	qs_walker_ctx->parent_plan_node_id = saved_parent_plan_node_id;
}

/*
 * qs_get_node_stats -- walker callback that snapshots one plan node.
 *
 * Allocates a GpscNodeSample in the current memory context, fills it from
 * planstate->instrument (if available), and appends it to
 * qs_walker_ctx->per_node_stats.
 *
 * Parameters:
 *   planstate      -- the plan node being sampled
 *   qs_walker_ctx  -- walker context; per_node_stats is extended in-place
 */
void
qs_get_node_stats(PlanState *planstate, QsWalkerContext *qs_walker_ctx)
{
	GpscNodeSample *nodestat =
		(GpscNodeSample *) palloc0(sizeof(GpscNodeSample));

	/* Identity fields. */
	gp_gettmid(&nodestat->tmid);
	nodestat->ssid    = gp_session_id;
	nodestat->ccnt    = gp_command_count;

	/* Plan-tree position. */
	nodestat->plan_node_id        = planstate->plan->plan_node_id;
	nodestat->parent_plan_node_id = qs_walker_ctx->parent_plan_node_id;
	nodestat->node_tag            = nodeTag(planstate->plan);
	nodestat->slice_id            = currentSliceId;
	nodestat->segindex            = GpIdentity.segindex;
	nodestat->dbid                = GpIdentity.dbid;

	/* Planner estimate. */
	nodestat->plan_rows = planstate->plan->plan_rows;

	/* Runtime instrumentation (may be NULL for non-instrumented nodes). */
	if (planstate->instrument)
	{
		Instrumentation *instr = planstate->instrument;

		if (qs_walker_ctx->finalize)
		{
			InstrEndLoop(instr);
		}

		nodestat->ntuples    = instr->ntuples + instr->tuplecount; /* include in-progress loop */
		nodestat->tuplecount = instr->tuplecount;
		nodestat->nloops     = instr->nloops;
		nodestat->startup    = instr->startup;
		nodestat->total      = instr->total;
		nodestat->firsttuple = instr->firsttuple;

		nodestat->shared_blks_hit  = instr->bufusage.shared_blks_hit;
		nodestat->shared_blks_read = instr->bufusage.shared_blks_read;

		/*
		 * eof lets a consumer tell a node that has finished producing (running
		 * but exhausted for this cycle) from one still actively pulling.  Only
		 * meaningful while running; nloops>0 / not-running already imply done.
		 */
		nodestat->eof = instr->eof;

		if (instr->running && !instr->eof)
			nodestat->node_status = QS_NODE_STATUS_EXECUTING;
		else if (instr->nloops > 0)
			nodestat->node_status = QS_NODE_STATUS_FINISHED;
		else
			nodestat->node_status = QS_NODE_STATUS_INITIALIZED;
		
		/*
		 * Per-node spill, from the GP-specific Instrumentation fields. These are
		 * populated by the node executors: workfileCreated live at spill for
		 * Agg/HashJoin/Hash, at eager-free for Sort; workmemused/workmemwanted at
		 * batch boundaries (Agg) or explain-end. A running snapshot is therefore a
		 * lower bound — consumers treat it as such.
		 */
		nodestat->workfile_created = instr->workfileCreated;
		nodestat->workmem_used     = (int64_t) instr->workmemused;
		nodestat->workmem_wanted   = (int64_t) instr->workmemwanted;
	}
	else
	{
		nodestat->node_status = QS_NODE_STATUS_INITIALIZED;
	}

	/*
	 * Populate relation_oid for scan nodes by looking up the range-table
	 * entry using the node's scanrelid.  EState.es_range_table is a flat
	 * List<RangeTblEntry *> indexed 1-based by scanrelid.
	 */
	switch (nodeTag(planstate->plan))
	{
		case T_SeqScan:
		case T_IndexScan:
		case T_IndexOnlyScan:
		case T_BitmapHeapScan:
		case T_TidScan:
		{
			Index scanrelid = ((Scan *) planstate->plan)->scanrelid;
			if (scanrelid > 0 && planstate->state != NULL)
			{
				List *rtable = planstate->state->es_range_table;
				if (scanrelid <= (Index) list_length(rtable))
				{
					RangeTblEntry *rte = (RangeTblEntry *)
						list_nth(rtable, (int) scanrelid - 1);
					if (rte->rtekind == RTE_RELATION)
						nodestat->relation_oid = (int32_t) rte->relid;
				}
			}
			break;
		}
		default:
			break;
	}

	qs_walker_ctx->per_node_stats =
		lappend(qs_walker_ctx->per_node_stats, nodestat);
}

/*
 * qs_debug_node_sample -- emit a single GpscNodeSample to the PostgreSQL LOG.
 *
 * Intended for development and integration testing.  In production deployments
 * this will produce a large number of log lines; suppress with log_min_messages.
 */
void
qs_debug_node_sample(GpscNodeSample *s)
{
	elog(DEBUG1,
		 "GpscNodeSample: "
		 "plan_node_id=%d parent=%d node_tag=%d "
		 "slice_id=%d segindex=%d "
		 "tmid=%d ssid=%d ccnt=%d "
		 "plan_rows=%.0f "
		 "ntuples=%.0f tuplecount=%.0f nloops=%.0f "
		 "startup=%f total=%f firsttuple=%f "
		 "shared_blks_hit=%lu shared_blks_read=%lu "
		 "workfile_created=%d workmem_used=%ld workmem_wanted=%ld "
		 "node_status=%d",
		 s->plan_node_id, s->parent_plan_node_id, s->node_tag,
		 s->slice_id, s->segindex,
		 s->tmid, s->ssid, s->ccnt,
		 s->plan_rows,
		 s->ntuples, s->tuplecount, s->nloops,
		 s->startup, s->total, s->firsttuple,
		 s->shared_blks_hit, s->shared_blks_read,
		 (int) s->workfile_created, (long) s->workmem_used, (long) s->workmem_wanted,
		 (int) s->node_status);
}

/*
 * qs_debug_node_stats -- emit all nodes in per_node_stats to the PostgreSQL LOG.
 *
 * Logs a summary line followed by one line per node via qs_debug_node_sample().
 */
void
qs_debug_node_stats(List *per_node_stats)
{
	ListCell *lc;
	int       i = 0;

	elog(DEBUG1, "GpscNodeSample list: %d nodes", list_length(per_node_stats));
	foreach(lc, per_node_stats)
	{
		GpscNodeSample *s = (GpscNodeSample *) lfirst(lc);
		elog(DEBUG1, "--- node[%d] ---", i++);
		qs_debug_node_sample(s);
	}
}

/*
 * runtime_explain -- snapshot the active query's plan tree.
 *
 * Retrieves the top-most QueryDesc from QueryDescStack, walks its planstate
 * tree with qs_get_node_stats(), and returns the resulting List of
 * GpscNodeSample pointers.
 *
 * Callers must ensure QueryDescStack is non-empty before calling this.
 */
static List *
runtime_explain(void)
{
	QsWalkerContext *qs_walker_ctx =
		(QsWalkerContext *) palloc0(sizeof(QsWalkerContext));
	QueryDesc *queryDesc;

	Assert(list_length(QueryDescStack) > 0);
	queryDesc = get_toppest_query();
	qs_planstate_walker(queryDesc->planstate, qs_get_node_stats,
						qs_walker_ctx, 0);
	return qs_walker_ctx->per_node_stats;
}

/*
 * SendQueryState -- handler for QueryStatePollReason.
 *
 * Fired asynchronously when another backend (or the monitoring function)
 * sends QueryStatePollReason to this process.
 *
 * Collects a plan-tree snapshot via runtime_explain() and emits it to the
 * PostgreSQL LOG via qs_debug_node_stats().  This branch does NOT push data
 * to any external sink.
 *
 * The entire body runs inside a dedicated MemoryContext that is deleted on
 * exit, preventing any leaks into the backend's long-lived contexts.  Any
 * errors are swallowed with FlushErrorState() to avoid crashing the backend.
 */
void
SendQueryState(void)
{
	MemoryContext oldcontext;
	MemoryContext qs_context;
	List         *qs_result = NIL;

	if (!pg_qs_enable)
		return;   /* STAT_DISABLED */

	if (!list_length(QueryDescStack))
		return;   /* QUERY_NOT_RUNNING */

	if (stack_is_too_deep())
	{
		elog(DEBUG1, "pg_query_state: skipping poll, call stack too deep");
		return;
	}

	qs_context = AllocSetContextCreate(TopMemoryContext,
									   "pg_query_state signal context",
									   ALLOCSET_DEFAULT_SIZES);
	oldcontext = MemoryContextSwitchTo(qs_context);

	PG_TRY();
	{
		qs_result = runtime_explain();
		qs_debug_node_stats(qs_result);
	}
	PG_CATCH();
	{
		FlushErrorState();
	}
	PG_END_TRY();

	MemoryContextSwitchTo(oldcontext);
	MemoryContextDelete(qs_context);
}

/*
 * SendCurrentUserId -- handler for UserIdPollReason.
 *
 * Sends a shm_mq_userid_msg containing the current effective user-id through
 * the shared mq so the requestor can verify the target backend's identity.
 */
void
SendCurrentUserId(void)
{
	shm_mq_handle     *mqh;
	shm_mq_userid_msg  msg;
	LOCKTAG            tag;

	msg.userid = GetUserId();

	LockShmem(&tag, PG_QS_SND_KEY);
	mqh        = shm_mq_attach(mq, NULL, NULL);
	msg.reqid  = *mq_req_id;

	if (shm_mq_get_sender(mq) != MyProc ||
		params->reason != UserIdPollReason)
	{
		elog(WARNING, "pg_query_state: SendCurrentUserId: stale or mismatched request");
	}
	else if (send_msg_by_parts(mqh, sizeof(msg), &msg) != MSG_BY_PARTS_SUCCEEDED)
	{
		elog(WARNING, "pg_query_state: SendCurrentUserId: failed to send reply");
	}

#if PG_VERSION_NUM < 100000
	shm_mq_detach(mq);
#else
	shm_mq_detach(mqh);
#endif
	UnlockShmem(&tag);
}

/*
 * fill_segpid -- populate consecutive gp_segment_pid slots from one CDB segment.
 *
 * Iterates the activelist of segInfo and fills msg->pids starting at *index,
 * incrementing *index for each entry.
 */
static void
fill_segpid(CdbComponentDatabaseInfo *segInfo, backend_info *msg, int *index)
{
	ListCell *lc;

	foreach(lc, segInfo->activelist)
	{
		SegmentDatabaseDescriptor *dbdesc =
			(SegmentDatabaseDescriptor *) lfirst(lc);
		gp_segment_pid *segpid = &msg->pids[(*index)++];
		segpid->pid   = dbdesc->backendPid;
		segpid->segid = dbdesc->segindex;
	}
}

/*
 * SendCdbComponents -- handler for BackendInfoPollReason (QD only).
 *
 * Collects the list of active QE (segid, pid) pairs from the CDB component
 * database and sends them back to the requestor through shm_mq as a
 * backend_info message.
 *
 * Side effects:
 *   - Calls cdbcomponent_getCdbComponents(), which may allocate memory.
 *   - All allocations are in a short-lived MemoryContext deleted on exit.
 */
void
SendCdbComponents(void)
{
	shm_mq_handle         *mqh = NULL;
	CdbComponentDatabases *cdbs;
	msg_by_parts_result   send_result;
	MemoryContext         oldctx;
	int                   index = 0;
	volatile int32        savedInterruptHoldoffCount;
	MemoryContext         query_state_ctx =
		AllocSetContextCreate(TopMemoryContext,
							  "pg_query_state SendCdbComponents",
							  ALLOCSET_DEFAULT_SIZES);

	oldctx = MemoryContextSwitchTo(query_state_ctx);
	savedInterruptHoldoffCount = InterruptHoldoffCount;

	PG_TRY();
	{
		mqh = shm_mq_attach(mq, NULL, NULL);

		if (shm_mq_get_sender(mq) != MyProc ||
			params->reason != BackendInfoPollReason)
		{
			elog(DEBUG1, "pg_query_state: SendCdbComponents: stale request, discarding");
			shm_mq_detach(mqh);
		}
		else if (!pg_qs_enable)
		{
			elog(DEBUG1, "pg_query_state: SendCdbComponents: module disabled");
			shm_mq_msg disabled_msg = {*mq_req_id, BASE_SIZEOF_SHM_MQ_MSG,
									   MyProc, STAT_DISABLED};
			if (send_msg_by_parts(mqh, disabled_msg.length,
								  &disabled_msg) != MSG_BY_PARTS_SUCCEEDED)
				shm_mq_detach(mqh);
		}
		else if (list_length(QueryDescStack) == 0)
		{
			elog(DEBUG1, "pg_query_state: SendCdbComponents: no active query");
			shm_mq_msg not_running_msg = {*mq_req_id, BASE_SIZEOF_SHM_MQ_MSG,
										  MyProc, QUERY_NOT_RUNNING};
			if (send_msg_by_parts(mqh, not_running_msg.length,
								  &not_running_msg) != MSG_BY_PARTS_SUCCEEDED)
				shm_mq_detach(mqh);
		}
		else
		{
			cdbs = cdbcomponent_getCdbComponents();
			int msglen = BASE_SIZEOF_GP_BACKEND_INFO +
						 sizeof(gp_segment_pid) * cdbs->numActiveQEs;
			backend_info *msg = (backend_info *) palloc0(msglen);

			msg->reqid       = *mq_req_id;
			msg->length      = msglen;
			msg->result_code = QS_RETURNED;

			for (int i = 0; i < cdbs->total_segment_dbs; i++)
			{
				CdbComponentDatabaseInfo *segInfo =
					&cdbs->segment_db_info[i];
				fill_segpid(segInfo, msg, &index);
			}
			Assert(index == cdbs->numActiveQEs);
			msg->number = index;

			send_result = send_msg_by_parts(mqh, msglen, msg);
			if (send_result != MSG_BY_PARTS_SUCCEEDED)
				shm_mq_detach(mqh);
		}
	}
	PG_CATCH();
	{
		elog(WARNING, "pg_query_state: SendCdbComponents: error during send");
		elog_dismiss(WARNING);
		if (mqh)
			shm_mq_detach(mqh);
		InterruptHoldoffCount = savedInterruptHoldoffCount;
	}
	PG_END_TRY();

	MemoryContextSwitchTo(oldctx);
	MemoryContextDelete(query_state_ctx);
}
