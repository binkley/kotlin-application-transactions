package hm.binkley.labs.applicationTransactions.processing

import hm.binkley.labs.applicationTransactions.CancelUnitOfWork
import hm.binkley.labs.applicationTransactions.FailureRemoteResult
import hm.binkley.labs.applicationTransactions.OneRead
import hm.binkley.labs.applicationTransactions.OneWrite
import hm.binkley.labs.applicationTransactions.ReadWorkUnit
import hm.binkley.labs.applicationTransactions.RemoteQuery
import hm.binkley.labs.applicationTransactions.RemoteRequest
import hm.binkley.labs.applicationTransactions.RemoteResult
import hm.binkley.labs.applicationTransactions.UnitOfWorkScope
import hm.binkley.labs.applicationTransactions.WriteWorkUnit
import hm.binkley.labs.applicationTransactions.client.RequestClient
import java.util.Queue
import java.util.concurrent.BlockingQueue
import java.util.concurrent.Executors.newCachedThreadPool
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
    /** The request queue shared with [RequestClient]. */
    requestQueue: BlockingQueue<RemoteRequest>,
    remoteResource: RemoteResource,
    /** TODO: An utterly generic idea of a logger. */
    private val logger: Queue<String>,
    /**
     * How long to wait to retry scanning for the next work unit.
     * This time is to detect when a unit of work does not make progress, and
     * open the remote resource to other queries.
     * Units of work failing this timeout are cancelled.
     * The default is to wait 1 second.
     */
    maxWaitForWorkUnitsInSeconds: Long = 1L,
    /**
     * How long to wait for the remote resource to complete a request
     * respecting that the remote resource may take a while for an
     * instruction/query.
     * The default is to wait 30 seconds.
     */
    private val maxWaitForRemoteResourceInSeconds: Long = 30L,
    /**
     * How long to wait for the remote resource to become idle.
     * If a remote resource is busy, this is the time to wait for it to free up.
     * The default is to wait 1 second.
     */
    waitBetweenRemoteRetriesInSeconds: Long = 1L,
) : Runnable {
    private val requestQueue = RequestQueue(
        sharedWithCallers = requestQueue,
        maxWaitForWorkUnitsInSeconds = maxWaitForWorkUnitsInSeconds,
    )
    private val remoteResource = RemoteResourceWithLinearBusyRetry(
        trueRemoteResource = remoteResource,
        maxTries = 2,
        waitBetweenRetriesInSeconds = waitBetweenRemoteRetriesInSeconds,
    )

    /**
     * Reader threads for the remote resource run in parallel.
     * The hard-coded policy is to use the JVM "cached thread pool".
     * This may change for other languages such as C#.
     */
    private val readerThreads = ReaderThreads(newCachedThreadPool())

    override fun run() {
        // The main loop processing client requests to the remote resource.
        // All other functions support this loop
        while (true) {
            when (val request = requestQueue.takeAnyNextRequest()) {
                is OneRead -> runParallelForSingleReads(request)

                /*
                 * All others need exclusive access to remote resource.
                 * These are blocking, run serial, and run on this thread
                 */

                is OneWrite -> runSerialExclusiveOfOtherRequests(request)

                // Canceling a unit of work before any reads/writes sent
                is CancelUnitOfWork -> cancelUnitOfWorkWithoutWork(request)

                is ReadWorkUnit, is WriteWorkUnit ->
                    runExclusiveForUnitOfWork(request as UnitOfWorkScope)
            }
        }
    }

    private fun runParallelForSingleReads(request: RemoteQuery) {
        readerThreads.submit { respondToClient(request) }
    }

    /**
     * If timing out waiting for readers to complete, does not execute the
     * write request.
     */
    private fun runSerialExclusiveOfOtherRequests(request: RemoteQuery) = when {
        waitForReadersToComplete() -> respondToClient(request)
        else -> readersDidNotFinishInTime(request)
    }

    private fun cancelUnitOfWorkWithoutWork(request: CancelUnitOfWork) {
        logBadWorkUnit(request)
        request.result.complete(true)
    }

    private fun runExclusiveForUnitOfWork(startWork: UnitOfWorkScope) {
        var currentWork = startWork

        /** Tracks the current work unit in case of bugs in the caller. */
        var expectedCurrent = 1

        while (true) {
            if (isBugInWorkUnit(startWork, currentWork, expectedCurrent)) {
                return
            }

            when (currentWork) {
                is CancelUnitOfWork -> {
                    runSerialForRollback(currentWork)

                    return
                }

                is ReadWorkUnit -> {
                    // Note: as this does not run in the current thread, we
                    // cannot check the return type (success vs failure), and
                    // the caller needs to send `CancelUnitOfWork` to stop
                    // processing of the unit of work early
                    runParallelForSingleReads(currentWork)
                }

                is WriteWorkUnit -> {
                    val result = runSerialExclusiveOfOtherRequests(currentWork)
                    if (result is FailureRemoteResult) {
                        return
                    }
                }
            }

            if (currentWork.isLastWorkUnit()) {
                return
            }

            val found = requestQueue.pollNextUnitOfWorkRequest(currentWork.id)
            if (null == found) {
                logSlowUnitOfWork(currentWork)
                return
            }

            currentWork = found
            ++expectedCurrent
        }
    }

    private fun respondToClient(request: RemoteQuery): RemoteResult {
        val result = remoteResource.call(request.query)
        if (result is FailureRemoteResult) {
            logFailedQueries(result)
        }

        request.result.complete(result)

        return result
    }

    /**
     * Only reads are submitted to the pool of reader threads.
     * Writes do not need waiting on: the code is structured so that writes
     * run in serial, and do not involve the reader threads (writes run on the
     * current thread).
     * However, as reads run in parallel, writes should wait for them to
     * complete (all reads run from the pool of reader threads).
     */
    private fun waitForReadersToComplete() = readerThreads.awaitCompletion(
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
            is CancelUnitOfWork -> if (!isGood) {
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
    private fun runSerialForRollback(request: CancelUnitOfWork) {
        if (!waitForReadersToComplete()) {
            return readersDidNotFinishInTime(request)
        }

        var outcome = true
        request.undo.forEach { query ->
            val result = remoteResource.call(query)
            if (result is FailureRemoteResult) {
                logFailedQueries(result)
                outcome = false
            }
        }

        request.result.complete(outcome)
    }

    private fun readersDidNotFinishInTime(
        request: RemoteQuery
    ): FailureRemoteResult {
        logSlowReaders()

        val result = remoteResultTimeout(maxWaitForRemoteResourceInSeconds)
        request.result.complete(result)

        return result
    }

    private fun readersDidNotFinishInTime(request: CancelUnitOfWork) {
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
        logger.offer(
            "TODO: Logging: SLOW WORK UNIT! -> last processed: $currentWork"
        )
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
private fun remoteResultTimeout(timeout: Long) = FailureRemoteResult(
    status = 504,
    query = "READ", // The exact query is in another thread and unknown
    errorMessage = "Timeout for a read after $timeout seconds",
)
