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
        top@ while (!interrupted()) {
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
                    // We're seeing an abandon before a transaction is begun
                    // with a read or write
                    // BUG: Log? Abandon without any reads/writes
                    request.result.complete(false)
                    continue
                }

                is WorkUnit -> {
                    // First unit of work -- starts the transaction.
                    // Runs in a loop looking for further work units in this
                    // transaction
                    var work = request as UnitOfWorkScope
                    val expected = work.expectedUnits
                    var current = 1

                    do {
                        if (work is AbandonUnitOfWork) {
                            runRollback(work)
                            continue@top // Break out of UoW
                        }

                        if (badUnitOfWorkRequest(expected, current, request)) {
                            continue@top // Break out of UoW
                        }

                        respondToClientInUnitOfWork(request)

                        if (work.isLastWorkUnit()) {
                            continue@top // Break out of UoW
                        }

                        when (val found = waitForNextWorkUnit(work.id)) {
                            null -> {
                                respondWithTimeout(request)
                                continue@top // Break out of UoW
                            }

                            else -> work = found
                        }

                        ++current
                    } while (current <= work.expectedUnits)
                }
            }
        }
    }

    private fun badUnitOfWorkRequest(
        expectedByProcessorFromFirstUnitOfWorkRequest: Int,
        currentInProcessor: Int,
        request: RemoteRequest,
    ): Boolean {
        val work = request as UnitOfWorkScope

        if (currentInProcessor == work.currentUnit &&
            expectedByProcessorFromFirstUnitOfWorkRequest >= work.currentUnit &&
            expectedByProcessorFromFirstUnitOfWorkRequest == work.expectedUnits
        ) {
            return false
        }

        when (work) {
            // TODO: Logging
            is AbandonUnitOfWork -> work.result.complete(false)

            is WorkUnit -> {
                respondWithBug(
                    work,
                    "Unit of work out of sequence or inconsistent:" +
                        " expected total calls: $expectedByProcessorFromFirstUnitOfWorkRequest;" +
                        " actual: ${work.expectedUnits};" +
                        " expected current call: $currentInProcessor;" +
                        " actual: ${work.currentUnit}" +
                        " (id: ${work.id})"
                )
            }
        }

        return true // Break out of UoW
    }

    private fun respondWithBug(request: WorkUnit, errorMessage: String) {
        // TODO: How to log in addition to responding to caller?
        request.result.complete(FailureRemoteResult(500, "BUG: $errorMessage"))
    }

    private fun respondWithTimeout(request: WorkUnit) {
        respondWithBug(
            request,
            "Next work unit not found within 1 second" +
                " last seen work unit: $request" +
                " (id: ${request.id})"
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
