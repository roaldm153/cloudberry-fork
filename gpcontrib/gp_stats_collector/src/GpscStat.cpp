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
 * GpscStat.cpp
 *
 * IDENTIFICATION
 *	  gpcontrib/gp_stats_collector/src/GpscStat.cpp
 *
 *-------------------------------------------------------------------------
 */

#include "GpscStat.h"

#include <algorithm>

extern "C" {
#include "postgres.h"
#include "miscadmin.h"
#include "storage/ipc.h"
#include "storage/lwlock.h"
#include "storage/shmem.h"
#include "storage/spin.h"
}

namespace
{

/*
 * ProtectedData -- spin-lock-protected wrapper around the GpscStat counters.
 *
 * Lives in a shared-memory segment so all backends on the same segment host
 * contribute to the same counters.
 */
struct ProtectedData
{
	slock_t        mutex;
	GpscStat::Data data;
};

static shmem_startup_hook_type prev_shmem_startup_hook = NULL;

/*
 * prev_shmem_request_hook is only relevant on PostgreSQL 15+, where the
 * shmem request phase is separate from the startup phase.
 */
#if PG_VERSION_NUM >= 150000
static shmem_request_hook_type prev_shmem_request_hook = NULL;

/*
 * gpsc_shmem_request -- request shared memory space.
 *
 * Installed as shmem_request_hook on PG15+.  Chains to the previous hook
 * before adding our own request.
 */
static void
gpsc_shmem_request()
{
	if (prev_shmem_request_hook)
		prev_shmem_request_hook();
	RequestAddinShmemSpace(sizeof(ProtectedData));
}
#endif  /* PG_VERSION_NUM >= 150000 */

static ProtectedData *data = nullptr;

/*
 * gpsc_shmem_startup -- attach to (or initialise) the GpscStat shared segment.
 *
 * Installed as shmem_startup_hook.  On first call (found == false) zeroes the
 * counters and initialises the spin lock.  Always chains to the previous hook.
 */
static void
gpsc_shmem_startup()
{
	if (prev_shmem_startup_hook)
		prev_shmem_startup_hook();

	LWLockAcquire(AddinShmemInitLock, LW_EXCLUSIVE);
	bool found;
	data = reinterpret_cast<ProtectedData *>(
		ShmemInitStruct("gpsc_stat_messages", sizeof(ProtectedData), &found));
	if (!found)
	{
		SpinLockInit(&data->mutex);
		data->data = GpscStat::Data();
	}
	LWLockRelease(AddinShmemInitLock);
}

/*
 * LockGuard -- RAII wrapper around a SpinLock.
 *
 * Acquires the spin lock on construction and releases it on destruction,
 * ensuring the lock is always released even if an exception is thrown.
 */
class LockGuard
{
public:
	explicit LockGuard(slock_t *mutex) : mutex_(mutex)
	{
		SpinLockAcquire(mutex_);
	}
	~LockGuard()
	{
		SpinLockRelease(mutex_);
	}

private:
	slock_t *mutex_;
};

}  // namespace

/*
 * GpscStat::init -- install shmem hooks during shared_preload_libraries phase.
 *
 * Must be called while process_shared_preload_libraries_in_progress is true.
 * On PostgreSQL 14 and earlier, shared memory is requested here directly via
 * RequestAddinShmemSpace().  On PostgreSQL 15+ a separate shmem_request_hook
 * handles the request.
 */
void
GpscStat::init()
{
	if (!process_shared_preload_libraries_in_progress)
		return;

#if PG_VERSION_NUM >= 150000
	prev_shmem_request_hook = shmem_request_hook;
	shmem_request_hook = gpsc_shmem_request;
#else
	RequestAddinShmemSpace(sizeof(ProtectedData));
#endif

	prev_shmem_startup_hook = shmem_startup_hook;
	shmem_startup_hook = gpsc_shmem_startup;
}

/*
 * GpscStat::deinit -- restore shmem hooks to their previous values.
 *
 * Called from hooks_deinit().
 */
void
GpscStat::deinit()
{
#if PG_VERSION_NUM >= 150000
	shmem_request_hook = prev_shmem_request_hook;
#endif
	shmem_startup_hook = prev_shmem_startup_hook;
}

/*
 * GpscStat::reset -- zero all counters in the shared segment.
 */
void
GpscStat::reset()
{
	LockGuard lg(&data->mutex);
	data->data = GpscStat::Data();
}

/*
 * GpscStat::report_send -- record a successful message send.
 *
 * Parameters:
 *   msg_size -- size of the sent protobuf message in bytes
 */
void
GpscStat::report_send(int32_t msg_size)
{
	LockGuard lg(&data->mutex);
	data->data.total++;
	data->data.max_message_size =
		std::max(msg_size, data->data.max_message_size);
}

/*
 * GpscStat::report_bad_connection -- record a failed UDS connection attempt.
 */
void
GpscStat::report_bad_connection()
{
	LockGuard lg(&data->mutex);
	data->data.total++;
	data->data.failed_connects++;
}

/*
 * GpscStat::report_bad_send -- record a failed send on an established connection.
 *
 * Parameters:
 *   msg_size -- size of the message that could not be sent
 */
void
GpscStat::report_bad_send(int32_t msg_size)
{
	LockGuard lg(&data->mutex);
	data->data.total++;
	data->data.failed_sends++;
	data->data.max_message_size =
		std::max(msg_size, data->data.max_message_size);
}

/*
 * GpscStat::report_error -- record any other error not covered by the above.
 */
void
GpscStat::report_error()
{
	LockGuard lg(&data->mutex);
	data->data.total++;
	data->data.failed_other++;
}

/*
 * GpscStat::get_stats -- return a snapshot of all counters.
 *
 * The snapshot is taken under the spin lock and returned by value, so the
 * caller sees a consistent view.
 */
GpscStat::Data
GpscStat::get_stats()
{
	LockGuard lg(&data->mutex);
	return data->data;
}

/*
 * GpscStat::loaded -- return true when the shared segment has been mapped.
 *
 * Returns false before shmem_startup_hook has run (e.g. if the extension was
 * not loaded via shared_preload_libraries).
 */
bool
GpscStat::loaded()
{
	return data != nullptr;
}
