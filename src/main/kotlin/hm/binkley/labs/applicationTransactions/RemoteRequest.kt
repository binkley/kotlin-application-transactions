package hm.binkley.labs.applicationTransactions

import hm.binkley.labs.applicationTransactions.client.UnitOfWork
import java.util.UUID
import java.util.concurrent.CompletableFuture

/**
 * All requests for the remote resource (reads/writes) are mediated.
 * The implementations of [RemoteRequest] provide these request operations:
 * - [OneRead] -- A single read request that can run in parallel with other
 * read requests
 * - [OneWrite] -- A single write request that runs exclusive of other
 * request operations
 * - [ReadWorkUnit] -- a single read request that is part of a [UnitOfWork]
 * and can run in parallel with other read requests
 * - [WriteWorkUnit] -- a single write request that is part of a [UnitOfWork]
 * and runs exclusive of other request operations
 * - [CancelUnitOfWork] -- leaves a unit of work
 */
sealed interface RemoteRequest

sealed interface RemoteQuery {
    val query: String

    /** Caller blocks obtaining the result until it is available. */
    val result: CompletableFuture<RemoteResult>
}

/**
 * A single read request outside a unit of work.
 * Remotely, it runs concurrently in parallel with other reads, including any
 * initial reads in a unit of work.
 */
data class OneRead(
    override val query: String,
    override val result: CompletableFuture<RemoteResult> = CompletableFuture(),
) : RemoteRequest, RemoteQuery

/**
 * A single write request outside a unit of work.
 * Remotely, it waits for reads to complete, and blocks other reads and writes
 * (including those in a unit of work).
 */
data class OneWrite(
    override val query: String,
    override val result: CompletableFuture<RemoteResult> = CompletableFuture(),
) : RemoteRequest, RemoteQuery

sealed interface UnitOfWorkScope {
    /** Common for all requests in the same unit of work. */
    val id: UUID

    /**
     * Remotely, automatically close the unit of work once processing this
     * number of requests.
     * This is akin to "auto-commit" with transactions.
     *
     * @todo Should this be combined with the same abstraction in unit of work?
     */
    val expectedUnits: Int

    /**
     * 1-based.
     * Used for auto-closing units of work when all expected requests are
     * processed, and for detecting misuse of the API.
     *
     * @todo Should this be combined with the same abstraction in unit of work?
     */
    val currentUnit: Int

    /** See [UnitOfWork.completed]. */
    fun isLastWorkUnit() = expectedUnits == currentUnit
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

/** Cancels a unit of work, optionally providing [undo] instructions. */
data class CancelUnitOfWork(
    override val id: UUID,
    override val expectedUnits: Int,
    /**
     * Rollback instructions, possibly empty.
     * All instructions are executed: if some fail, rollback continues with
     * later instructions.
     */
    val undo: List<String> = emptyList(),
) : RemoteRequest, UnitOfWorkScope {
    override val currentUnit = expectedUnits

    /** Did all undo instructions succeed? */
    val result = CompletableFuture<Boolean>()
}
