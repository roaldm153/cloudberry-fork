#ifndef PLAN_NODE_EMITTER_H
#define PLAN_NODE_EMITTER_H

#ifdef __cplusplus
extern "C" {
#endif

#include "pg_query_state/qs_types.h"

/*
 * gpsc_emit_plan_batch -- serialize an array of GpscNodeSample as a single
 * yagpcc::SetPerNodeBatchReq and send it to the yagpcc UDS sink.
 *
 * Opens one UDS connection for the whole backend instead of one per node.
 * The shared identity keys (query_key, segment_key) and datetime are taken
 * from nodes[0] and hoisted out of every node.  A count of 0 is a no-op.
 *
 * Must be called from C++ translation units only (the implementation
 * includes generated protobuf headers).
 */
extern void gpsc_emit_plan_batch(GpscNodeSample **nodes, int count);

/*
 * gpsc_emit_query_plan -- send a coordinator-only plan document to the yagpcc
 * UDS sink as a yagpcc::SetQueryPlanReq (extended protocol, request_type=2).
 *
 * Should be called only on the QD (Gp_role == GP_ROLE_DISPATCH).  plan_doc is
 * the ExplainPrintPlan output; format is the ExplainFormat used to render it
 * (0=text, 1=xml, 2=json, 3=yaml).  A NULL or empty plan_doc is a no-op.
 *
 * Must be called from C++ translation units only.
 */
extern void gpsc_emit_query_plan(int32_t tmid, int32_t ssid, int32_t ccnt,
								 const char *plan_doc, int32_t format);

/*
 * gpsc_qs_sync_config -- reload the UDS path and other connector settings
 * from the Config singleton before a gpsc_emit_plan_batch() call.
 *
 * Call once per signal/hook invocation before emitting.
 */
extern void gpsc_qs_sync_config();

#ifdef __cplusplus
}
#endif

#endif /* PLAN_NODE_EMITTER_H */
