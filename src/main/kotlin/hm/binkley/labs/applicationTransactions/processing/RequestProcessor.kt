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
    private val remoteResourceManager: RemoteResourceManager,
    /** An utterly generic idea of a logger. */
    private val logger: Queue<String>,
    /** How long to wait to retry scanning for the next work unit. */
    private val maxWaitForWorkUnitsInSeconds: Long = 1L,
    /** How long to wait for the remote resource to complete a read. */
    private val maxWaitForRemoteResourceInSeconds: Long = 30L,
) : Runnable {
    private val workerPool = WorkerPool(threadPool)

    override fun run() {
        // The main loop processing client requests to the remote resource.
        // All other functions support this loop
        while (!Thread.interrupted()) {
            // TODO: Nicer would be "take" instead of "poll", and block until
            //  there is a request available
            when (val request = requestQueue.poll()) {
                null -> {
                    /* Do-nothing busy loop waiting for new requests */
                }

                is OneRead -> runParallelForReads(request)

                /*
                 * All others need exclusive access to remote resource.
                 * These are blocking, run serial, and run on this thread
                 */

                is OneWrite -> runSerialForWrites(request)

                is AbandonUnitOfWork, is ReadWorkUnit, is WriteWorkUnit ->
                    runExclusiveForUnitOfWork(request as UnitOfWorkScope)
            }
        }
    }

    private fun runParallelForReads(request: RemoteQuery) {
        workerPool.submit { respondToClient(request) }
    }

    /**
     * If timing out waiting for readers to complete, does not execute the
     * write request.
     */
    private fun runSerialForWrites(request: RemoteQuery) =
        if (!waitForReadersToComplete()) {
            readersDidNotFinishInTime(request)
        } else {
            respondToClient(request)
        }

    private fun runExclusiveForUnitOfWork(startWork: UnitOfWorkScope) {
        var currentWork = startWork

        /** Tracks the current work unit in case of bugs in the caller. */
        var expectedCurrent = 1

        while (!Thread.interrupted()) {
            if (isBugInWorkUnit(startWork, currentWork, expectedCurrent)) {
                return
            }

            when (currentWork) {
                is AbandonUnitOfWork -> {
                    runSerialForRollback(currentWork)

                    return
                }

                is ReadWorkUnit -> {
                    // Note: as this does not run in the current thread, we
                    // cannot check the return type (success vs failure), and
                    // the caller needs to send `AbandonUnitOfWork` to stop
                    // processing of the unit of work
                    runParallelForReads(currentWork)
                }

                is WriteWorkUnit -> {
                    val result = runSerialForWrites(currentWork)
                    if (result is FailureRemoteResult) {
                        return
                    }
                }
            }

            if (currentWork.isLastWorkUnit()) {
                return
            }

            val found = awaitNextWorkUnit(currentWork.id)
            if (null == found) {
                logSlowUnitOfWork(currentWork)
                return
            }

            currentWork = found
            ++expectedCurrent
        }
    }

    private fun respondToClient(request: RemoteQuery): RemoteResult {
        val result = remoteResourceManager.callWithBusyRetry(request.query)
        if (result is FailureRemoteResult) {
            logFailedQueries(result)
        }

        request.result.complete(result)

        return result
    }

    /**
     * Only reads are submitted to the worker pool.
     * Writes do not need waiting on: the code is structured so that writes
     * always run in serial, never overlapping, and do not involve the worker
     * pool (all writes run on the current thread).
     * However, reads run in parallel, so writes should wait for them to
     * complete (all reads run from the worker pool).
     */
    private fun waitForReadersToComplete() =
        workerPool.awaitCompletion(
            maxWaitForRemoteResourceInSeconds,
            SECONDS
        )

    /**
     * Checks for a work unit not part of the current unit of work.
     * The only current case is: current item is not in sequence.
     *
     * The cases of bad id or wrong expected total number of items are
     * handled when looping through the request queue for the next item.
     *
     * Note the difference between abandon and read/write: abandon "jumps to
     * the end" whereas read/write plods on one at a time.
     */
    private fun isBugInWorkUnit(
        startWork: UnitOfWorkScope,
        currentWork: UnitOfWorkScope,
        expectedCurrent: Int,
    ): Boolean {
        // Check caller is on the same page
        var isGood = startWork.expectedUnits == currentWork.expectedUnits
        when (currentWork) {
            is AbandonUnitOfWork -> if (!isGood) {
                logBadWorkUnit(currentWork)

                currentWork.result.complete(false)
            }

            is ReadWorkUnit, is WriteWorkUnit -> {
                // READ/WRITE should proceed 1 at a time
                isGood = isGood && expectedCurrent == currentWork.currentUnit
                if (!isGood) {
                    logBadWorkUnit(currentWork)

                    respondToClientWithBug(
                        startWork,
                        currentWork as RemoteQuery,
                        expectedCurrent,
                    )
                }
            }
        }

        return !isGood
    }

    /**
     * If timing out waiting for readers to complete, does not execute the
     * undo instructions.
     */
    private fun runSerialForRollback(request: AbandonUnitOfWork) {
        if (!waitForReadersToComplete()) {
            return readersDidNotFinishInTime(request)
        }

        var outcome = true
        request.undo.forEach { query ->
            val result = remoteResourceManager.callWithBusyRetry(query)
            if (result is FailureRemoteResult) {
                logFailedQueries(result)
                outcome = false
            }
        }

        request.result.complete(outcome)
    }

    private fun awaitNextWorkUnit(id: UUID): UnitOfWorkScope? {
        val found = pollForNextWorkUnit(id)
        if (null != found) return found

        // Let client process some, and try again
        SECONDS.sleep(maxWaitForWorkUnitsInSeconds)

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

    private fun readersDidNotFinishInTime(request: RemoteQuery): FailureRemoteResult {
        logSlowReaders()

        val result = remoteResultTimeout(maxWaitForRemoteResourceInSeconds)
        request.result.complete(result)

        return result
    }

    private fun readersDidNotFinishInTime(request: AbandonUnitOfWork) {
        logSlowReaders()

        request.result.complete(false)
    }

    /** @todo Logging */
    private fun logSlowReaders() {
        logger.offer("TODO: Logging: SLOW READERS!")
    }

    /** @todo Logging */
    private fun logFailedQueries(result: RemoteResult) {
        logger.offer("TODO: Logging: FAILED QUERY! -> $result")
    }

    /** @todo Logging */
    private fun logBadWorkUnit(currentWork: UnitOfWorkScope) {
        logger.offer("TODO: Logging: BAD WORK UNIT! -> $currentWork")
    }

    /** @todo Logging */
    private fun logSlowUnitOfWork(currentWork: UnitOfWorkScope) {
        logger.offer("TODO: Logging: SLOW WORK UNIT! -> $currentWork")
    }
}

private fun respondToClientWithBug(
    startWork: UnitOfWorkScope,
    currentWorkUnit: RemoteQuery,
    expectedCurrent: Int,
) {
    currentWorkUnit.result.complete(
        FailureRemoteResult(
            500,
            currentWorkUnit.query,
            "BUG: Bad work unit" +
                " [expected id: ${startWork.id};" +
                " expected total work units: ${startWork.expectedUnits};" +
                " expected current item: $expectedCurrent]: " +
                " request: $currentWorkUnit"

        )
    )
}

/** Returns a failure when the remote resource times out. */
private fun remoteResultTimeout(timeout: Long) =
    FailureRemoteResult(
        status = 504,
        query = "READ", // The exact query is in another thread and unknown
        errorMessage = "Timeout for a read after $timeout seconds",
    )
