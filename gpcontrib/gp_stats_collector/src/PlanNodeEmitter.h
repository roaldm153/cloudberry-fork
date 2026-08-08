#ifndef PLAN_NODE_EMITTER_H
#define PLAN_NODE_EMITTER_H

#ifdef __cplusplus
extern "C" {
#endif

#include "pg_query_state/qs_types.h"

extern void gpsc_emit_node_batch(GpscNodeSample **nodes, int count,
								 const char *trace_id);
extern void gpsc_emit_query_plan(int32_t tmid, int32_t ssid, int32_t ccnt,
								 const char *plan_doc, int32_t format);
extern void gpsc_qs_sync_config();

#ifdef __cplusplus
}
#endif

#endif /* PLAN_NODE_EMITTER_H */
