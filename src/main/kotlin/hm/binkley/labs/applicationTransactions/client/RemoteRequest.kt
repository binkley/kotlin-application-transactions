package hm.binkley.labs.applicationTransactions.client

import java.util.*

sealed interface RemoteRequest

/**
 * A single read request outside a unit of work.
 * Remotely, it runs concurrently with other reads.
 */
data class OneRead(val query: String) : RemoteRequest

/**
 * A single write request outside a unit of work.
 * Remotely, it runs serially, and blocks other requests.
 */
data class OneWrite(val query: String) : RemoteRequest

/** A single request within a unit of work. */
interface WorkUnit : RemoteRequest {
    val id: UUID

    /**
     * Remotely, automatically close the unit of work once processing this
     * number of requests.
     * This is akin to "auto-commit".
     */
    val expectedUnits: Int

    /** 1-based */
    val currentUnit: Int
    val query: String
}

/**
 * A single read request inside a unit of work.
 * Remotely, it runs concurrently with other reads, but not those from other
 * units of work (they are blocked).
 */
data class ReadWorkUnit(
    override val id: UUID,
    override val expectedUnits: Int,
    override val currentUnit: Int,
    override val query: String,
) : WorkUnit

/**
 * A single write request inside a unit of work.
 * Remotely, it runs serially, and blocks other requests.
 */
data class WriteWorkUnit(
    override val id: UUID,
    override val expectedUnits: Int,
    override val currentUnit: Int,
    override val query: String,
) : WorkUnit

/** Remotely abandons a unit of work. */
class AbandonUnitOfWork(val id: UUID) : RemoteRequest
