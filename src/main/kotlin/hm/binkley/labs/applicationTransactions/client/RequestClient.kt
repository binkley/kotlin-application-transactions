package hm.binkley.labs.applicationTransactions.client

import hm.binkley.labs.applicationTransactions.FailureRemoteResult
import hm.binkley.labs.applicationTransactions.OneRead
import hm.binkley.labs.applicationTransactions.OneWrite
import hm.binkley.labs.applicationTransactions.RemoteQuery
import hm.binkley.labs.applicationTransactions.RemoteRequest
import hm.binkley.labs.applicationTransactions.SuccessRemoteResult
import java.util.concurrent.BlockingQueue

/** Convenience class for sending requests to the remote resource. */
class RequestClient(private val requestQueue: BlockingQueue<RemoteRequest>) {
    /**
     * Sends a single simple read request to a remote resource.
     * Reads run in parallel.
     */
    fun readOne(query: String) = runRequest(OneRead(query))

    /**
     * Sends a single simple, exclusive write request to a remote resource.
     * Writes run serially and exclusive of other requests.
     */
    fun writeOne(query: String) = runRequest(OneWrite(query))

    /**
     * Starts an exclusive session with the remote resource employing a
     * [UnitOfWork].
     * Exclusive sessions run serially and exclusive of other requests.
     */
    fun inExclusiveAccess(expectedUnits: Int): Transactionish<String, Unit> =
        ExclusiveAccess(expectedUnits)

    private inner class ExclusiveAccess(expectedUnits: Int) :
        Transactionish<String, Unit> {
        private val uow = UnitOfWork(expectedUnits)

        /**
         * Sends a single read request to a remote resource.
         * These may run in parallel with other outstanding read requests,
         * including those started prior to exclusive access.
         */
        override fun readOne(query: String) = runRequest(uow.readOne(query))

        /**
         * Sends a single write request to a remote resource.
         * Writes run serially and exclusive of other requests.
         */
        override fun writeOne(query: String) = runRequest(uow.writeOne(query))

        /** Cancels the current exclusive access to the remote resource. */
        override fun cancel() {
            requestQueue.offer(uow.cancel())
        }

        /**
         * Aborts the current exclusive access to the remote resource with
         * instructions for undoing changes.
         */
        override fun abort(undo: List<String>) {
            requestQueue.offer(uow.abort(undo))
        }

        override fun close() = uow.close()
    }

    private fun <T> runRequest(request: T): String
        where T : RemoteRequest,
              T : RemoteQuery {
        requestQueue.offer(request)
        return when (val result = request.result.get()) { // Blocking
            is SuccessRemoteResult -> result.response
            is FailureRemoteResult -> handleFailure(result)
        }
    }
}

/**
 * @todo Unclear if calling users should use exceptions, or should drive
 *       solely off the status code in the remote result
 */
private fun handleFailure(result: FailureRemoteResult): Nothing =
    error("TODO: Implement an exception scheme: $result")
