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
 * UDSConnector.h
 *
 * IDENTIFICATION
 *	  gpcontrib/gp_stats_collector/src/UDSConnector.h
 *
 *-------------------------------------------------------------------------
 */

#ifndef UDSCONNECTOR_H
#define UDSCONNECTOR_H

#include "protos/gpsc_set_service.pb.h"
#include "protos/yagpcc_set_per_node.pb.h"

class Config;

/*
 * UDSConnector -- thin static helper that sends protobuf messages over a
 * Unix-domain socket.
 *
 * All methods open a fresh non-blocking SOCK_STREAM connection, serialise
 * the protobuf, write it with the appropriate wire header, and close the
 * socket.  They return true on success and false on any error (the error
 * is also recorded via GpscStat).
 */
class UDSConnector
{
public:
	/*
	 * report_query -- send a SetQueryReq using the original 4-byte length header.
	 *
	 * Parameters:
	 *   req    -- the populated request message
	 *   event  -- human-readable event name used in error log messages
	 *   config -- current connector config (UDS path, etc.)
	 */
	bool static report_query(const gpsc::SetQueryReq &req,
							 const std::string &event, const Config &config);

	/*
	 * report_per_node_batch -- send a yagpcc::SetPerNodeBatchReq using the
	 * 8-byte extended protocol header with request_type = 1.
	 *
	 * One whole plan-tree snapshot per call: a single socket open/write/close
	 * for the entire backend instead of one per node.
	 *
	 * Wire format:
	 *   bytes 0-3: payload_size | kExtendedProtocolFlag  (uint32 LE)
	 *   bytes 4-5: request_type = 1                       (uint16 LE)
	 *   bytes 6-7: reserved = 0                           (uint16 LE)
	 *   bytes 8+:  serialized SetPerNodeBatchReq
	 *
	 * Parameters:
	 *   req    -- the populated batch request message
	 *   config -- current connector config (UDS path, etc.)
	 */
	bool static report_per_node_batch(const yagpcc::SetPerNodeBatchReq &req,
									  const Config &config);

	/*
	 * report_query_plan -- send a yagpcc::SetQueryPlanReq using the 8-byte
	 * extended protocol header with request_type = 2.
	 *
	 * Carries the coordinator-only ExplainPrintPlan document.  Same framing as
	 * the other extended messages; only the request_type byte differs.
	 *
	 * Parameters:
	 *   req    -- the populated plan-doc request message
	 *   config -- current connector config (UDS path, etc.)
	 */
	bool static report_query_plan(const yagpcc::SetQueryPlanReq &req,
								  const Config &config);
};

#endif /* UDSCONNECTOR_H */
