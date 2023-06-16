package hm.binkley.labs.applicationTransactions.client

import java.util.*
import java.util.concurrent.CompletableFuture
import java.util.concurrent.Future

sealed interface RemoteRequest

interface RemoteQuery : RemoteRequest {
    val query: String

    /** Caller blocks obtaining the result until it is available. */
    val result: Future<RemoteResult>
}

/**
 * A single read request outside a unit of work.
 * Remotely, it runs concurrently with other reads.
 */
data class OneRead(
    override val query: String,
    override val result: CompletableFuture<RemoteResult> =
        CompletableFuture(),
) : RemoteQuery

/**
 * A single write request outside a unit of work.
 * Remotely, it runs serially, and blocks other requests.
 */
data class OneWrite(
    override val query: String,
    override val result: CompletableFuture<RemoteResult> =
        CompletableFuture(),
) : RemoteQuery

/** A single request within a unit of work. */
interface WorkUnit : RemoteQuery {
    val id: UUID

    /**
     * Remotely, automatically close the unit of work once processing this
     * number of requests.
     * This is akin to "auto-commit".
     */
    val expectedUnits: Int

    /** 1-based */
    val currentUnit: Int
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
    override val result: CompletableFuture<RemoteResult> =
        CompletableFuture(),
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
    override val result: CompletableFuture<RemoteResult> =
        CompletableFuture(),
) : WorkUnit

/**
 * Remotely abandons a unit of work.
 * There is no response from remote.
 */
class AbandonUnitOfWork(
    val id: UUID,
    val undo: List<String> = emptyList(),
) : RemoteRequest
