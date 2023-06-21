package hm.binkley.labs.applicationTransactions

import java.util.UUID
import java.util.concurrent.CompletableFuture

sealed interface RemoteRequest

/**
 * @todo Drop [WorkUnit] and use union types
 */
interface RemoteQuery {
    val query: String

    /** Caller blocks obtaining the result until it is available. */
    val result: CompletableFuture<RemoteResult>
}

/**
 * A single read request outside a unit of work.
 * Remotely, it runs concurrently with other reads.
 */
data class OneRead(override val query: String) : RemoteRequest, RemoteQuery {
    override val result = CompletableFuture<RemoteResult>()
}

/**
 * A single write request outside a unit of work.
 * Remotely, it runs serially, and blocks other requests.
 */
data class OneWrite(override val query: String) : RemoteRequest, RemoteQuery {
    override val result = CompletableFuture<RemoteResult>()
}

interface UnitOfWorkScope {
    val id: UUID

    /**
     * Remotely, automatically close the unit of work once processing this
     * number of requests.
     * This is akin to "auto-commit".
     */
    val expectedUnits: Int

    /** 1-based */
    val currentUnit: Int
}

/** A single request within a unit of work. */
interface WorkUnit : RemoteRequest, RemoteQuery, UnitOfWorkScope {
    override val query: String
    override val result: CompletableFuture<RemoteResult>
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
) : WorkUnit {
    override val result = CompletableFuture<RemoteResult>()
}

/**
 * A single write request inside a unit of work.
 * Remotely, it runs serially, and blocks other requests.
 */
data class WriteWorkUnit(
    override val id: UUID,
    override val expectedUnits: Int,
    override val currentUnit: Int,
    override val query: String,
) : WorkUnit {
    override val result = CompletableFuture<RemoteResult>()
}

/**
 * Remotely abandons a unit of work.
 * There is no response from remote.
 */
data class AbandonUnitOfWork(
    override val id: UUID,
    override val expectedUnits: Int,
    override val currentUnit: Int,
    val undo: List<String> = emptyList(),
) : RemoteRequest, UnitOfWorkScope {
    val result = CompletableFuture<Boolean>()
}
