package hm.binkley.labs.applicationTransactions.processing

import hm.binkley.labs.applicationTransactions.AbandonUnitOfWork
import hm.binkley.labs.applicationTransactions.FailureRemoteResult
import hm.binkley.labs.applicationTransactions.OneRead
import hm.binkley.labs.applicationTransactions.OneWrite
import hm.binkley.labs.applicationTransactions.RemoteQuery
import hm.binkley.labs.applicationTransactions.RemoteRequest
import hm.binkley.labs.applicationTransactions.RemoteResult
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
    private val retryQueueForWorkInSeconds: Long = 1L,
    private val retryRemoteInSeconds: Long = 1L,
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
                    val startWork = request as UnitOfWorkScope
                    var currentWork = request as UnitOfWorkScope
                    var expectedCurrent = 1

                    while (true) {
                        if (currentWork is AbandonUnitOfWork) {
                            runRollback(currentWork)
                            continue@top // Break out of UoW
                        }

                        if (badWorkUnit(
                                startWork as WorkUnit,
                                currentWork as WorkUnit,
                                expectedCurrent,
                            )
                        ) {
                            // TODO: BUG: Logging
                            println("BAD WORK UNIT! -> $request")
                            continue@top // Break out of UoW
                        }

                        respondToClientInUnitOfWork(currentWork as RemoteQuery)

                        // Break out of UoW
                        if (currentWork.isLastWorkUnit()) continue@top

                        // Break out of UoW
                        when (val found = waitForNextWorkUnit(currentWork.id)) {
                            // TODO: Log that caller is too slow in calling
                            //  again?
                            //  There is no request to respond to
                            null -> continue@top // Break out of UoW
                            else -> currentWork = found
                        }

                        ++expectedCurrent
                    }
                }
            }
        }
    }

    private fun respondWithBug(request: WorkUnit, errorMessage: String) {
        // TODO: How to log in addition to responding to caller?
        request.result.complete(FailureRemoteResult(500, "BUG: $errorMessage"))
    }

    private fun badWorkUnit(
        startWork: WorkUnit,
        work: WorkUnit,
        expectedCurrent: Int,
    ): Boolean {
        if (startWork.id == work.id &&
            startWork.expectedUnits == work.expectedUnits &&
            expectedCurrent == work.currentUnit
        ) {
            return false
        }

        respondWithBug(
            work,
            "Bad work unit" +
                " [expected id: ${startWork.id};" +
                " expected total work units: ${startWork.expectedUnits};" +
                " expected current work: $expectedCurrent]: " +
                " request: $work"
        )

        return true // Break out of UoW
    }

    private fun tryCallingRemote(
        query: String,
    ): RemoteResult {
        val response = remoteResource.call(query)
        if (429 != response.status) return response

        // Retry remote after waiting
        SECONDS.sleep(retryRemoteInSeconds)

        return remoteResource.call(query)
    }

    private fun respondToClient(request: RemoteQuery) =
        request.result.complete(tryCallingRemote(request.query))

    private fun respondToClientInUnitOfWork(request: RemoteQuery) {
        if (request is WriteWorkUnit) waitForReadersToComplete()
        respondToClient(request)
    }

    private fun runRollback(request: AbandonUnitOfWork) {
        var outcome = true
        request.undo.forEach { query ->
            // TODO: Logging? Return outcomes to caller?
            if (tryCallingRemote(query) is FailureRemoteResult) {
                outcome = false
            }
        }
        request.result.complete(outcome)
    }

    private fun waitForReadersToComplete() {
        workerPool.awaitCompletion(retryQueueForWorkInSeconds, SECONDS)
    }

    private fun waitForNextWorkUnit(id: UUID): UnitOfWorkScope? {
        val found = pollForNextWorkUnit(id)
        if (null != found) return found

        // Let client process some, and try again
        SECONDS.sleep(retryQueueForWorkInSeconds)

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
