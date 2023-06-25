package hm.binkley.labs.applicationTransactions

import java.util.UUID
import java.util.concurrent.CompletableFuture

sealed interface RemoteRequest

sealed interface RemoteQuery {
    val query: String

    /** Caller blocks obtaining the result until it is available. */
    val result: CompletableFuture<RemoteResult>
}

/**
 * A single read request outside a unit of work.
 * Remotely, it runs concurrently with other reads.
 */
data class OneRead(
    override val query: String,
    override val result: CompletableFuture<RemoteResult> = CompletableFuture(),
) : RemoteRequest, RemoteQuery

/**
 * A single write request outside a unit of work.
 * Remotely, it runs serially, and blocks other requests.
 */
data class OneWrite(
    override val query: String,
    override val result: CompletableFuture<RemoteResult> = CompletableFuture(),
) : RemoteRequest, RemoteQuery

sealed interface UnitOfWorkScope {
    val id: UUID

    /**
     * Remotely, automatically close the unit of work once processing this
     * number of requests.
     * This is akin to "auto-commit".
     */
    val expectedUnits: Int

    /** 1-based */
    val currentUnit: Int

    /** See `UnitOfWork.complete`. */
    fun isLastWorkUnit() = expectedUnits == currentUnit
}

/** Abandons a unit of work, optionally running undo instructions. */
data class AbandonUnitOfWork(
    override val id: UUID,
    override val expectedUnits: Int,
    override val currentUnit: Int,
    val undo: List<String> = emptyList(),
) : RemoteRequest, UnitOfWorkScope {
    /** Did all undo instructions succeed? */
    val result = CompletableFuture<Boolean>()
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
    override val result: CompletableFuture<RemoteResult> = CompletableFuture(),
) : RemoteRequest, RemoteQuery, UnitOfWorkScope

/**
 * A single write request inside a unit of work.
 * Remotely, it runs serially, and blocks other requests.
 */
data class WriteWorkUnit(
    override val id: UUID,
    override val expectedUnits: Int,
    override val currentUnit: Int,
    override val query: String,
    override val result: CompletableFuture<RemoteResult> = CompletableFuture(),
) : RemoteRequest, RemoteQuery, UnitOfWorkScope
