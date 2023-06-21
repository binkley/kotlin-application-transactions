package hm.binkley.labs.applicationTransactions.processing

import hm.binkley.labs.applicationTransactions.AbandonUnitOfWork
import hm.binkley.labs.applicationTransactions.FailureRemoteResult
import hm.binkley.labs.applicationTransactions.OneRead
import hm.binkley.labs.applicationTransactions.OneWrite
import hm.binkley.labs.applicationTransactions.RemoteQuery
import hm.binkley.labs.applicationTransactions.RemoteRequest
import hm.binkley.labs.applicationTransactions.UnitOfWorkScope
import hm.binkley.labs.applicationTransactions.WorkUnit
import hm.binkley.labs.applicationTransactions.WriteWorkUnit
import java.lang.Thread.interrupted
import java.util.Queue
import java.util.UUID
import java.util.concurrent.ExecutorService
import java.util.concurrent.TimeUnit.SECONDS

/**
 * Note the embedded timeouts.
 * These need review, and moving out into configuration.
 */
class RequestProcessor(
    private val requestQueue: Queue<RemoteRequest>,
    threadPool: ExecutorService,
    private val remoteResource: RemoteResource,
) : Runnable {
    private val workerPool = WorkerPool(threadPool)

    override fun run() { // Never exits until process shut down
        while (!interrupted()) {
            when (val request = requestQueue.poll()) {
                null -> continue // Busy loop for new requests

                is OneRead -> {
                    // Reads outside a unit of work runs in parallel
                    workerPool.submit { respondToClient(request) }
                    continue
                }

                // All others need exclusive access to remote resource
                // These are blocking, run serial, and run on this thread

                is OneWrite -> {
                    waitForReadersToComplete()
                    respondToClient(request)
                    continue
                }

                is AbandonUnitOfWork -> {
                    // TODO: Remove this case and let it fall to the next one
                    // BUG: Log? Abandon without any reads/writes
                    // There is no client completion for this
                    request.result.complete(false)
                    continue
                }

                is WorkUnit -> {
                    // First unit of work -- starts the transaction.
                    // Runs in a loop looking for further work units
                    var work = request as UnitOfWorkScope
                    var current = 1

                    do {
                        if (work is AbandonUnitOfWork) {
                            runRollback(work)
                            break
                        }

                        if (current != work.currentUnit) {
                            failWithBugOutOfOrderWorkUnits(request, current)
                            break
                        }

                        respondToClientInUnitOfWork(request)

                        when (val found = waitForNextWorkUnit(work.id)) {
                            null -> {
                                failWithBugSlowClient(request)
                                break
                            }

                            else -> work = found
                        }

                        ++current
                    } while (current <= work.expectedUnits)
                }
            }
        }
    }

    private fun failWithBugOutOfOrderWorkUnits(
        request: WorkUnit,
        current: Int,
    ) {
        request.result.complete(
            FailureRemoteResult(
                500,
                "BUG: Unit of work out of sequence:" +
                    " expected $current; actual: ${request.currentUnit}" +
                    " (id: ${request.id})"
            )
        )
    }

    private fun failWithBugSlowClient(
        request: WorkUnit,
    ) {
        request.result.complete(
            FailureRemoteResult(
                500,
                "BUG: Next work unit not found within 1 second" +
                    " (id: ${request.id})"
            )
        )
    }

    private fun respondToClient(request: RemoteQuery) =
        request.result.complete(remoteResource.call(request.query))

    private fun respondToClientInUnitOfWork(request: RemoteQuery) {
        if (request is WriteWorkUnit) waitForReadersToComplete()
        respondToClient(request)
    }

    private fun runRollback(request: AbandonUnitOfWork) {
        var outcome = true
        request.undo.forEach { query ->
            // TODO: Logging? Return to caller?
            if (remoteResource.call(query) is FailureRemoteResult) {
                outcome = false
            }
        }
        request.result.complete(outcome)
    }

    private fun waitForReadersToComplete() {
        workerPool.awaitCompletion(1L, SECONDS)
    }

    private fun waitForNextWorkUnit(id: UUID): UnitOfWorkScope? {
        val found = pollForNextWorkUnit(id)
        if (null != found) return found

        SECONDS.sleep(1L) // Let client process some, and try again

        return pollForNextWorkUnit(id)
    }

    /**
     * Find the next request queue item that is:
     * 1. A unit of work
     * 2. Has [id] for that unit of work
     * and removes that item from the queue
     *
     * @return the first matching request, or `null` if none found
     */
    private fun pollForNextWorkUnit(id: UUID): UnitOfWorkScope? {
        val itr = requestQueue.iterator()
        while (itr.hasNext()) {
            val request = itr.next()
            if (request !is UnitOfWorkScope || id != request.id) continue

            itr.remove()
            return request
        }
        return null
    }
}
