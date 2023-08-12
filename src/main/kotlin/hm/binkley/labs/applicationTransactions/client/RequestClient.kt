package hm.binkley.labs.applicationTransactions.client

import hm.binkley.labs.applicationTransactions.FailureRemoteResult
import hm.binkley.labs.applicationTransactions.OneRead
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
         * Sends a single read request to a remote resource within a unit of
         * work.
         * These may run in parallel with other outstanding read requests,
         * including those started prior to exclusive access.
         */
        override fun readOne(query: String) = runRequest(uow.readOne(query))

        /**
         * Sends a single write request to a remote resource within a unit of
         * work.
         * Writes run serially and exclusive of other requests.
         */
        override fun writeOne(query: String) = runRequest(uow.writeOne(query))

        /** Cancels the current exclusive access to the remote resource. */
        override fun cancelAndKeepChanges() {
            requestQueue.offer(uow.cancelAndKeepChanges())
        }

        /**
         * Cancels the current exclusive access to the remote resource with
         * instructions for undoing changes.
         */
        override fun cancelAndUndoChanges(undo: List<String>) {
            requestQueue.offer(uow.cancelAndUndoChanges(undo))
        }

        /** @todo Consider automatically sending a cancel request */
        override fun close() = uow.close()
    }

    private fun <T> runRequest(request: T): String
        where T : RemoteRequest,
              T : RemoteQuery {
        requestQueue.offer(request)
        return when (val result = request.result.get()) { // Blocking
            is SuccessRemoteResult -> result.response
            is FailureRemoteResult -> throw ExampleFrameworkException(
                result.status,
                result.query,
                result.errorMessage
            )
        }
    }
}
