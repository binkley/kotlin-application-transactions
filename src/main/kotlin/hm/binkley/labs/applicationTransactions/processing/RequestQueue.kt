package hm.binkley.labs.applicationTransactions.processing

import hm.binkley.labs.applicationTransactions.RemoteRequest
import hm.binkley.labs.applicationTransactions.UnitOfWorkScope
import java.util.UUID
import java.util.concurrent.BlockingQueue
import java.util.concurrent.TimeUnit.SECONDS

internal class RequestQueue(
    private val sharedWithCallers: BlockingQueue<RemoteRequest>,
    private val maxWaitForWorkUnitsInSeconds: Long,
    private val spilloverList: MutableList<RemoteRequest> = mutableListOf(),
) {
    fun takeAnyNextRequest(): RemoteRequest =
        if (spilloverList.isNotEmpty()) {
            spilloverList.removeFirst()
        } else {
            sharedWithCallers.take()
        }

    fun pollNextUnitOfWorkRequest(uowId: UUID): UnitOfWorkScope? {
        fun isCurrentUnitOfWork(request: RemoteRequest) =
            request is UnitOfWorkScope && uowId == request.id

        // More efficient would be to walk an iterator over the spillover list,
        // and remove the element if matching, but that is less portable for
        // languages like C# which do not support iterators that mutate
        var nextRequest = spilloverList.firstOrNull(::isCurrentUnitOfWork)
        if (null != nextRequest) {
            spilloverList.remove(nextRequest)
            return nextRequest as UnitOfWorkScope
        }

        while (true) {
            nextRequest =
                sharedWithCallers.poll(maxWaitForWorkUnitsInSeconds, SECONDS)
            when {
                null == nextRequest -> return null

                isCurrentUnitOfWork(nextRequest) ->
                    return nextRequest as UnitOfWorkScope

                else -> spilloverList.add(nextRequest)
            }
        }
    }
}
