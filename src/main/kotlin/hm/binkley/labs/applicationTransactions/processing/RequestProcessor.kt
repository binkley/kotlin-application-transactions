package hm.binkley.labs.applicationTransactions.processing

import hm.binkley.labs.applicationTransactions.AbandonUnitOfWork
import hm.binkley.labs.applicationTransactions.FailureRemoteResult
import hm.binkley.labs.applicationTransactions.OneRead
import hm.binkley.labs.applicationTransactions.OneWrite
import hm.binkley.labs.applicationTransactions.ReadWorkUnit
import hm.binkley.labs.applicationTransactions.RemoteQuery
import hm.binkley.labs.applicationTransactions.RemoteRequest
import hm.binkley.labs.applicationTransactions.RemoteResult
import hm.binkley.labs.applicationTransactions.UnitOfWorkScope
import hm.binkley.labs.applicationTransactions.WorkUnit
import hm.binkley.labs.applicationTransactions.WriteWorkUnit
import java.util.Queue
import java.util.UUID
import java.util.concurrent.ExecutorService
import java.util.concurrent.TimeUnit.SECONDS

/**
 * Top-level request processor running on a dedicated thread in an infinite
 * loop (until the thread is interrupted).
 *
 * Assumptions and conditions:
 * - Retry the remote resource _once_ when it is busy the first try
 * - Transactions progress speedily:
 *   when a client caller is slow, cancel the unit of work
 */
class RequestProcessor(
    private val requestQueue: Queue<RemoteRequest>,
    threadPool: ExecutorService,
    private val remoteResource: RemoteResource,
    /** How long to wait for the remote resource to become idle. */
    private val awaitRemoteInSeconds: Long = 1L,
    /** How long to wait to retry scanning for the next work unit. */
    private val retryRequestQueueForWorkUnitsInSeconds: Long = 1L,
) : Runnable {
    private val workerPool = WorkerPool(threadPool)

    override fun run() { // Never exits until process shut down
        while (!Thread.interrupted()) {
            // TODO: Nicer would be "take" instead of "poll", and block until
            //  there is a request available
            when (val request = requestQueue.poll()) {
                null -> {
                    /* Do-nothing busy loop waiting for new requests */
                }

                is OneRead -> {
                    runParallelForSimpleReads(request)
                }

                /*
                 * All others need exclusive access to remote resource.
                 * These are blocking, run serial, and run on this thread
                 */

                is OneWrite -> {
                    runExclusiveForSimpleWrites(request)
                }

                is AbandonUnitOfWork, is ReadWorkUnit, is WriteWorkUnit -> {
                    runExclusiveForUnitOfWork(request as UnitOfWorkScope)
                }
            }
        }
    }

    private fun runParallelForSimpleReads(request: OneRead) {
        workerPool.submit { respondToClient(request) }
    }

    private fun runExclusiveForSimpleWrites(request: OneWrite) {
        waitForAllToComplete()
        respondToClient(request)
    }

    private fun runExclusiveForUnitOfWork(startWork: UnitOfWorkScope) {
        var currentWork = startWork
        var expectedCurrent = 1

        while (true) {
            if (currentWork is AbandonUnitOfWork) {
                runRollback(currentWork)
                return
            }

            val currentWorkUnit = currentWork as WorkUnit
            if (badWorkUnit(
                    startWork,
                    currentWorkUnit,
                    expectedCurrent,
                )
            ) {
                logBadWorkUnit(currentWork)
                respondToClientWithBug(
                    startWork,
                    currentWorkUnit,
                    expectedCurrent
                )
                return
            }

            respondToClientInUnitOfWork(
                currentWorkUnit as RemoteQuery
            )

            if (currentWorkUnit.isLastWorkUnit()) {
                return
            }

            val found = awaitNextWorkUnit(currentWorkUnit.id)
            if (null == found) {
                logSlowUnitOfWork(currentWork)
                return
            }
            currentWork = found
            ++expectedCurrent
        }
    }

    private fun respondToClient(request: RemoteQuery) =
        request.result.complete(tryCallingRemote(request.query))

    private fun respondToClientInUnitOfWork(request: RemoteQuery) {
        /**
         * Subtle point: when a unit of work begins with read requests, those
         * can continue in parallel with outstanding simple reads.
         * Only when encountering writes do we need exclusive access to the
         * remote resource.
         */
        if (request is WriteWorkUnit) waitForAllToComplete()
        respondToClient(request)
    }

    private fun respondToClientWithBug(
        startWork: UnitOfWorkScope,
        currentWorkUnit: WorkUnit,
        expectedCurrent: Int,
    ) {
        currentWorkUnit.result.complete(
            FailureRemoteResult(
                500,
                "BUG: Bad work unit" +
                    " [expected id: ${startWork.id};" +
                    " expected total work units: ${startWork.expectedUnits};" +
                    " expected current item: $expectedCurrent]: " +
                    " request: $currentWorkUnit"

            )
        )
    }

    private fun tryCallingRemote(
        query: String,
    ): RemoteResult {
        val response = remoteResource.call(query)
        if (429 != response.status) return response

        // Retry remote after waiting
        SECONDS.sleep(awaitRemoteInSeconds)

        return remoteResource.call(query)
    }

    private fun waitForAllToComplete() {
        workerPool.awaitCompletion(
            retryRequestQueueForWorkUnitsInSeconds,
            SECONDS
        )
    }

    /**
     * Checks for a work unit not part of the current unit of work.
     * The only current case is: current item is not in sequence.
     *
     * The cases of bad id or wrong expected total number of items are
     * handled when looping through the request queue for next item.
     *
     * @todo Refactor and enhance tests so that this check becomes unneeded
     */
    private fun badWorkUnit(
        startWork: UnitOfWorkScope,
        currentWorkUnit: WorkUnit,
        expectedCurrent: Int,
    ) = !(
        startWork.id == currentWorkUnit.id &&
            startWork.expectedUnits == currentWorkUnit.expectedUnits &&
            expectedCurrent == currentWorkUnit.currentUnit
        )

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

    private fun awaitNextWorkUnit(id: UUID): UnitOfWorkScope? {
        val found = pollForNextWorkUnit(id)
        if (null != found) return found

        // Let client process some, and try again
        SECONDS.sleep(retryRequestQueueForWorkUnitsInSeconds)

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

/** @todo Logging */
private fun logBadWorkUnit(currentWork: UnitOfWorkScope) {
    println("TODO: Logging: BAD WORK UNIT! -> $currentWork")
}

/** @todo Logging */
private fun logSlowUnitOfWork(currentWork: UnitOfWorkScope) {
    println("TODO: Logging: SLOW WORK UNIT! -> $currentWork")
}
