package hm.binkley.labs.applicationTransactions.processing

import hm.binkley.labs.applicationTransactions.AbandonUnitOfWork
import hm.binkley.labs.applicationTransactions.OneRead
import hm.binkley.labs.applicationTransactions.OneWrite
import hm.binkley.labs.applicationTransactions.ReadWorkUnit
import hm.binkley.labs.applicationTransactions.RemoteQuery
import hm.binkley.labs.applicationTransactions.RemoteRequest
import hm.binkley.labs.applicationTransactions.WriteWorkUnit
import java.lang.Thread.interrupted
import java.util.Queue
import java.util.concurrent.ExecutorService
import java.util.concurrent.TimeUnit.SECONDS

/**
 * Note the embedded timeouts.
 * These need review, and moving out into configuration.
 */
class RequestProcessor(
    private val requestQueue: Queue<RemoteRequest>,
    private val remoteResource: RemoteResource,
    threadPool: ExecutorService,
) : Runnable {
    private val workerPool = WorkerPool(threadPool)

    override fun run() { // Never exits until process shut down
        while (!interrupted()) {
            when (val request = requestQueue.poll()) {
                null -> continue // Busy loop looking for new requests
                // Reads outside a unit of work run in parallel
                is OneRead -> workerPool.submit {
                    respondToClient(request)
                }
                // All others need exclusive access to remote resource
                // These are blocking and run serial, and run on this thread
                is OneWrite -> {
                    workerPool.awaitCompletion(1L, SECONDS)
                    respondToClient(request)
                }
                is ReadWorkUnit -> TODO()
                is WriteWorkUnit -> TODO()
                is AbandonUnitOfWork -> TODO()
            }
        }
    }

    private fun respondToClient(request: RemoteQuery) =
        request.result.complete(remoteResource.call(request.query))
}
