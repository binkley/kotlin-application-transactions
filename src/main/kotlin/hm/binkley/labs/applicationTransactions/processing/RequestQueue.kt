package hm.binkley.labs.applicationTransactions.processing

import hm.binkley.labs.applicationTransactions.RemoteRequest
import hm.binkley.labs.applicationTransactions.UnitOfWorkScope
import hm.binkley.labs.util.SearchableBlockingQueue
import java.util.UUID
import java.util.concurrent.BlockingQueue
import java.util.concurrent.TimeUnit.SECONDS

internal class RequestQueue(
    sharedWithCallers: BlockingQueue<RemoteRequest>,
    private val maxWaitForWorkUnitsInSeconds: Long,
) {
    private val queue = SearchableBlockingQueue(sharedWithCallers)

    fun takeAnyNextRequest(): RemoteRequest = queue.take()

    fun pollNextUnitOfWorkRequest(uowId: UUID): UnitOfWorkScope? =
        queue.poll(maxWaitForWorkUnitsInSeconds, SECONDS) { request ->
            request is UnitOfWorkScope && uowId == request.id
        } as UnitOfWorkScope?
}
