/*
 * qs_types.h
 *		Per-node sample type collected by the pg_query_state plan-tree walker.
 *
 * Copyright (c) 2024, Postgres Professional
 *
 * IDENTIFICATION
 *	  gpcontrib/gp_stats_collector/src/pg_query_state/qs_types.h
 */
#ifndef QS_TYPES_H
#define QS_TYPES_H

#include <stdint.h>

/*
 * Execution phase of a single plan node as observed at signal time.
 */
typedef enum QsNodeStatus
{
	QS_NODE_STATUS_UNSPECIFIED = 0,
	QS_NODE_STATUS_INITIALIZED = 1,  /* instrumentation allocated but not yet started */
	QS_NODE_STATUS_EXECUTING   = 2,  /* currently inside a tuple-fetch call */
	QS_NODE_STATUS_FINISHED    = 3   /* at least one full loop completed */
} QsNodeStatus;

/*
 * Per-node snapshot collected by qs_get_node_stats().
 *
 * All timing fields mirror the PostgreSQL Instrumentation struct and carry
 * the same semantics: startup/total/firsttuple are in seconds,
 * ntuples/tuplecount/nloops are raw counters.
 *
 * relation_oid is populated for scan nodes (SeqScan, IndexScan,
 * IndexOnlyScan, BitmapHeapScan, TidScan) by reading the range-table entry
 * via EState.es_range_table.  It is zero for all other node types.
 */
typedef struct GpscNodeSample
{
	int32_t tmid;                    /* transaction/time id (gp_gettmid) */
	int32_t ssid;                    /* gp_session_id */
	int32_t ccnt;                    /* gp_command_count */
	int32_t plan_node_id;            /* Plan.plan_node_id */
	int32_t parent_plan_node_id;     /* plan_node_id of logical parent */
	int32_t node_tag;                /* nodeTag(plan) */
	int32_t slice_id;                /* currentSliceId */
	int32_t segindex;                /* GpIdentity.segindex */
	int32_t dbid;					 /* GpIdentity.dbid */
	int32_t relation_oid;            /* OID of scanned relation, or 0 */
	double  plan_rows;               /* optimizer row estimate */
	double  ntuples;                 /* Instrumentation.ntuples */
	double  tuplecount;              /* Instrumentation.tuplecount (in-progress loop) */
	double  nloops;                  /* Instrumentation.nloops */
	double  startup;                 /* Instrumentation.startup (seconds) */
	double  total;                   /* Instrumentation.total (seconds) */
	double  firsttuple;              /* Instrumentation.firsttuple (seconds) */
	uint64_t shared_blks_hit;
	uint64_t shared_blks_read;
	QsNodeStatus node_status;
	bool eof;						 /* Instrumentation.eof: node exhausted for
									  * the current cycle (last fetch returned no
									  * tuple).  Lets consumers tell a finished
									  * node from one still actively producing. */
	/*
	 * Spill, from the GP-specific Instrumentation fields. Reliable once the node
	 * is finalized; a mid-run snapshot is a lower bound (Sort/HashJoin populate
	 * these only at eager-free / explain-end).
	 */
	bool workfile_created;           /* Instrumentation.workfileCreated */
	int64_t workmem_used;            /* Instrumentation.workmemused (bytes) */
	int64_t workmem_wanted;          /* Instrumentation.workmemwanted (bytes); >0 == spilled */
} GpscNodeSample;

#endif /* QS_TYPES_H */
