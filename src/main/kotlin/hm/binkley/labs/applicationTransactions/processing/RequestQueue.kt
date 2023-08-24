package hm.binkley.labs.applicationTransactions.processing

import hm.binkley.labs.applicationTransactions.RemoteRequest
import hm.binkley.labs.applicationTransactions.UnitOfWorkScope
import hm.binkley.labs.applicationTransactions.client.RequestClient
import hm.binkley.labs.util.SearchableBlockingQueue
import java.util.UUID
import java.util.concurrent.BlockingQueue
import java.util.concurrent.TimeUnit.SECONDS

internal class RequestQueue(
    /** The request queue shared with [RequestClient]. */
    sharedWithCallers: BlockingQueue<RemoteRequest>,
    /** How long a client may take for a new request during a unit of work. */
    private val maxWaitForWorkUnitsInSeconds: Long,
) {
    private val queue = SearchableBlockingQueue(sharedWithCallers)

    /** Takes the most recent request in FIFO order. */
    fun takeAnyNextRequest(): RemoteRequest = queue.take()

    /** Polls for the next request part of a current unit of work. */
    fun pollNextUnitOfWorkRequest(uowId: UUID): UnitOfWorkScope? =
        queue.poll(maxWaitForWorkUnitsInSeconds, SECONDS) { request ->
            request is UnitOfWorkScope && uowId == request.id
        } as UnitOfWorkScope?
}
