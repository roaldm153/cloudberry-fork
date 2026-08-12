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
 * UDSConnector.cpp
 *
 * IDENTIFICATION
 *	  gpcontrib/gp_stats_collector/src/UDSConnector.cpp
 *
 *-------------------------------------------------------------------------
 */

#include "UDSConnector.h"
#include "Config.h"
#include "GpscStat.h"
#include "log/LogOps.h"
#include "memory/gpdbwrappers.h"

#include <cerrno>
#include <ctime>
#include <poll.h>
#include <string>
#include <sys/fcntl.h>
#include <sys/socket.h>
#include <sys/types.h>
#include <sys/un.h>
#include <unistd.h>

extern "C" {
#include "postgres.h"
}

/*
 * Extended protocol constants for the 8-byte header messages.
 *
 * kExtendedProtocolFlag is ORed into the 32-bit payload size field to signal
 * to the receiver that this is an extended (8-byte) header rather than the
 * original 4-byte header.
 *
 * The request-type word then selects the payload message:
 * kRequestTypePerNodeBatch -> yagpcc::SetPerNodeBatchReq,
 * kRequestTypeQueryPlan    -> yagpcc::SetQueryPlanReq.
 */
static const uint32_t kExtendedProtocolFlag    = 0x80000000u;
static const uint16_t kRequestTypePerNodeBatch = 1;
static const uint16_t kRequestTypeQueryPlan    = 2;

/*
 * Bound on how long open_nonblocking_uds waits for a non-blocking connect() to
 * finish when it returns EINPROGRESS/EAGAIN.  Kept short: a local UDS handshake
 * completes almost immediately, and we would rather drop a sample than stall the
 * backend during a connection burst.
 */
static const int kConnectTimeoutMs = 50;

/*
 * Overall bound on how long send_all keeps retrying a message when the UDS send
 * buffer stays full (EAGAIN).  Backpressure is normal, but we must not block the
 * backend indefinitely behind a stuck reader -- past this deadline we give up
 * and drop the message.
 */
static const int kSendTimeoutMs = 200;

/* ----------------------------------------------------------------
 * Error-logging helpers
 * ---------------------------------------------------------------- */

/*
 * log_query_failure -- emit a LOG message for a failed SetQueryReq send.
 *
 * Includes the query key (tmid/ssid/ccnt) and the triggering event name.
 */
static void inline
log_query_failure(const gpsc::SetQueryReq &req, const std::string &event)
{
	ereport(LOG, (errmsg("Query {%d-%d-%d} %s tracing failed with error %m",
						 req.query_key().tmid(), req.query_key().ssid(),
						 req.query_key().ccnt(), event.c_str())));
}

/*
 * log_per_node_batch_failure -- emit a LOG message for a failed
 * SetPerNodeBatchReq send.  Includes the hex trace_id and node count.
 */
static void inline
log_per_node_batch_failure(const yagpcc::SetPerNodeBatchReq &req)
{
	static const char hexchars[] = "0123456789abcdef";
	const std::string &tid = req.trace_id();
	std::string hex;

	hex.reserve(tid.size() * 2);
	for (unsigned char c : tid)
	{
		hex.push_back(hexchars[c >> 4]);
		hex.push_back(hexchars[c & 0x0f]);
	}

	ereport(LOG, (errmsg("Per-node batch {trace_id=%s} tracing (%d nodes) failed with error %m",
						 hex.c_str(), req.nodes_size())));
}

/*
 * log_query_plan_failure -- emit a LOG message for a failed SetQueryPlanReq
 * send.  Includes the query key and plan-doc byte size.
 */
static void inline
log_query_plan_failure(const yagpcc::SetQueryPlanReq &req)
{
	ereport(LOG, (errmsg("Query {%d-%d-%d} plan-doc tracing (%zu bytes) failed with error %m",
						 req.query_key().tmid(), req.query_key().ssid(),
						 req.query_key().ccnt(), req.plan_doc().size())));
}

/* ----------------------------------------------------------------
 * Socket helpers
 * ---------------------------------------------------------------- */

/*
 * open_nonblocking_uds -- create and connect a non-blocking AF_UNIX socket.
 *
 * Fills `address` from `uds_path`, creates a SOCK_STREAM socket, sets
 * O_NONBLOCK, and connects.  Returns the file descriptor on success.
 * On any failure, records the error in GpscStat, logs at WARNING/LOG level,
 * and returns -1.
 *
 * Parameters:
 *   uds_path -- filesystem path of the listening Unix-domain socket
 *   address  -- caller-supplied sockaddr_un to fill (must be zero-initialised)
 */
static int
open_nonblocking_uds(const std::string &uds_path, sockaddr_un &address)
{
	if (uds_path.size() >= sizeof(address.sun_path))
	{
		ereport(WARNING, (errmsg("UDS path is too long for socket buffer")));
		GpscStat::report_error();
		return -1;
	}
	address.sun_family = AF_UNIX;
	strcpy(address.sun_path, uds_path.c_str());

	const int sockfd = socket(AF_UNIX, SOCK_STREAM, 0);
	if (sockfd == -1)
	{
		GpscStat::report_error();
		return -1;
	}

	if (fcntl(sockfd, F_SETFL, O_NONBLOCK) == -1)
	{
		ereport(WARNING,
				(errmsg("Unable to create non-blocking socket connection %m")));
		GpscStat::report_error();
		close(sockfd);
		return -1;
	}

	if (connect(sockfd, reinterpret_cast<sockaddr *>(&address),
				sizeof(address)) == -1)
	{
		if (errno != EINPROGRESS && errno != EAGAIN)
		{
			GpscStat::report_bad_connection();
			close(sockfd);
			return -1;
		}

		pollfd pfd = {};
		pfd.fd	   = sockfd;
		pfd.events = POLLOUT;

		const int rc = poll(&pfd, 1, kConnectTimeoutMs);
		if (rc <= 0)
		{
			GpscStat::report_bad_connection();
			close(sockfd);
			return -1;
		}

		int so_error = 0;
		socklen_t len = sizeof(so_error);
		if (getsockopt(sockfd, SOL_SOCKET, SO_ERROR, &so_error, &len) == -1 ||
			so_error != 0)
		{
			GpscStat::report_bad_connection();
			close(sockfd);
			return -1;
		}
	}

	return sockfd;
}

/*
 * monotonic_ms -- current CLOCK_MONOTONIC reading in milliseconds.
 *
 * Used for send_all's retry deadline; monotonic so it is immune to wall-clock
 * jumps (NTP steps, settimeofday).
 */
static int64_t
monotonic_ms(void)
{
	struct timespec ts;
	clock_gettime(CLOCK_MONOTONIC, &ts);
	return (int64_t) ts.tv_sec * 1000 + ts.tv_nsec / 1000000;
}

/*
 * send_all -- write all `total_size` bytes from `buf` over `sockfd`.
 *
 * The socket is non-blocking and send() uses MSG_DONTWAIT.  A full send buffer
 * reports EAGAIN/EWOULDBLOCK -- normal backpressure, not an error -- so on that
 * we wait (bounded by kSendTimeoutMs) for the socket to become writable via
 * poll(POLLOUT) and retry, rather than dropping the message.  EINTR retries
 * immediately.  Returns true once every byte is sent, false on a hard error or
 * once the deadline is exceeded (errno is preserved on the hard-error path).
 */
static bool
send_all(int sockfd, const uint8_t *buf, size_t total_size)
{
	size_t        sent_total = 0;
	const int64_t deadline   = monotonic_ms() + kSendTimeoutMs;

	while (sent_total < total_size)
	{
		const ssize_t sent = send(sockfd, buf + sent_total,
								  total_size - sent_total, MSG_DONTWAIT);
		if (sent > 0)
		{
			sent_total += (size_t) sent;
			continue;
		}

		/* errno is only meaningful when send() actually failed (sent < 0). */
		if (sent < 0 && errno == EINTR)
			continue;
		if (sent < 0 && errno != EAGAIN && errno != EWOULDBLOCK)
			return false; /* hard error */

		/* Send buffer full (or a 0-byte return): wait, bounded, for drain. */
		const int64_t remaining = deadline - monotonic_ms();
		if (remaining <= 0)
			return false; /* sustained backpressure: give up */

		pollfd pfd = {};
		pfd.fd	   = sockfd;
		pfd.events = POLLOUT;

		const int rc = poll(&pfd, 1, (int) remaining);
		if (rc < 0 && errno == EINTR)
			continue;
		if (rc <= 0)
			return false; /* poll error or timeout */
	}

	return true;
}

/* ----------------------------------------------------------------
 * Public methods
 * ---------------------------------------------------------------- */

/*
 * UDSConnector::report_query -- send a SetQueryReq with a 4-byte length header.
 *
 * Wire format:
 *   bytes 0-3: payload_size (uint32 LE, no flags)
 *   bytes 4+:  serialized gpsc::SetQueryReq
 *
 * Parameters:
 *   req    -- populated request message
 *   event  -- label used in error log messages
 *   config -- current connector config (UDS path, etc.)
 *
 * Returns true on success, false on any failure.
 */
bool
UDSConnector::report_query(const gpsc::SetQueryReq &req,
						   const std::string &event, const Config &config)
{
	sockaddr_un address{};
	const auto &uds_path = config.uds_path();

	const int sockfd = open_nonblocking_uds(uds_path, address);
	if (sockfd == -1)
	{
		log_query_failure(req, event);
		return false;
	}

	struct SockGuard { int fd; ~SockGuard() { close(fd); } } sock_guard{sockfd};

	const auto data_size  = req.ByteSizeLong();
	const auto total_size = data_size + sizeof(uint32_t);
	auto *buf = static_cast<uint8_t *>(gpdb::palloc(total_size));
	struct BufGuard { void *p; ~BufGuard() { gpdb::pfree(p); } } buf_guard{buf};

	*reinterpret_cast<uint32_t *>(buf) = static_cast<uint32_t>(data_size);
	req.SerializeWithCachedSizesToArray(buf + sizeof(uint32_t));

	if (!send_all(sockfd, buf, total_size))
	{
		log_query_failure(req, event);
		GpscStat::report_bad_send(total_size);
		return false;
	}

	GpscStat::report_send(total_size);
	return true;
}

/*
 * UDSConnector::report_per_node_batch -- send a SetPerNodeBatchReq with the
 * 8-byte extended protocol header and request_type = 1.
 *
 * One socket open/write/close carries the whole
 * plan-tree snapshot for a backend.
 *
 * Parameters:
 *   req    -- populated batch request message
 *   config -- current connector config (UDS path, etc.)
 *
 * Returns true on success, false on any failure.
 */
bool
UDSConnector::report_per_node_batch(const yagpcc::SetPerNodeBatchReq &req,
									const Config &config)
{
	sockaddr_un address{};
	const auto &uds_path = config.uds_path();

	const int sockfd = open_nonblocking_uds(uds_path, address);
	if (sockfd == -1)
	{
		log_per_node_batch_failure(req);
		return false;
	}

	struct SockGuard { int fd; ~SockGuard() { close(fd); } } sock_guard{sockfd};

	const auto  data_size    = req.ByteSizeLong();
	const auto  header_size  = sizeof(uint32_t) + sizeof(uint16_t) + sizeof(uint16_t);
	const auto  total_size   = header_size + data_size;
	auto *buf = static_cast<uint8_t *>(gpdb::palloc(total_size));
	struct BufGuard { void *p; ~BufGuard() { gpdb::pfree(p); } } buf_guard{buf};

	/* Write the 8-byte extended header. */
	uint8_t *p = buf;
	*reinterpret_cast<uint32_t *>(p) =
		static_cast<uint32_t>(data_size) | kExtendedProtocolFlag;
	p += sizeof(uint32_t);
	*reinterpret_cast<uint16_t *>(p) = kRequestTypePerNodeBatch;
	p += sizeof(uint16_t);
	*reinterpret_cast<uint16_t *>(p) = 0;  /* reserved */
	p += sizeof(uint16_t);

	req.SerializeWithCachedSizesToArray(p);

	if (!send_all(sockfd, buf, total_size))
	{
		log_per_node_batch_failure(req);
		GpscStat::report_bad_send(total_size);
		return false;
	}

	GpscStat::report_send(total_size);
	return true;
}

/*
 * UDSConnector::report_query_plan -- send a SetQueryPlanReq with the 8-byte
 * extended protocol header and request_type = 2.
 *
 * Same framing as report_per_node_batch(); only the request_type byte and the
 * message type differ.
 *
 * Parameters:
 *   req    -- populated plan-doc request message
 *   config -- current connector config (UDS path, etc.)
 *
 * Returns true on success, false on any failure.
 */
bool
UDSConnector::report_query_plan(const yagpcc::SetQueryPlanReq &req,
								const Config &config)
{
	sockaddr_un address{};
	const auto &uds_path = config.uds_path();

	const int sockfd = open_nonblocking_uds(uds_path, address);
	if (sockfd == -1)
	{
		log_query_plan_failure(req);
		return false;
	}

	struct SockGuard { int fd; ~SockGuard() { close(fd); } } sock_guard{sockfd};

	const auto  data_size    = req.ByteSizeLong();
	const auto  header_size  = sizeof(uint32_t) + sizeof(uint16_t) + sizeof(uint16_t);
	const auto  total_size   = header_size + data_size;
	auto *buf = static_cast<uint8_t *>(gpdb::palloc(total_size));
	struct BufGuard { void *p; ~BufGuard() { gpdb::pfree(p); } } buf_guard{buf};

	/* Write the 8-byte extended header. */
	uint8_t *p = buf;
	*reinterpret_cast<uint32_t *>(p) =
		static_cast<uint32_t>(data_size) | kExtendedProtocolFlag;
	p += sizeof(uint32_t);
	*reinterpret_cast<uint16_t *>(p) = kRequestTypeQueryPlan;
	p += sizeof(uint16_t);
	*reinterpret_cast<uint16_t *>(p) = 0;  /* reserved */
	p += sizeof(uint16_t);

	req.SerializeWithCachedSizesToArray(p);

	if (!send_all(sockfd, buf, total_size))
	{
		log_query_plan_failure(req);
		GpscStat::report_bad_send(total_size);
		return false;
	}

	GpscStat::report_send(total_size);
	return true;
}
