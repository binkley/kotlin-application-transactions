package hm.binkley.labs.applicationTransactions.client

import hm.binkley.labs.applicationTransactions.FailureRemoteResult
import hm.binkley.labs.applicationTransactions.OneRead
import hm.binkley.labs.applicationTransactions.OneWrite
import hm.binkley.labs.applicationTransactions.RemoteQuery
import hm.binkley.labs.applicationTransactions.RemoteRequest
import hm.binkley.labs.applicationTransactions.SuccessRemoteResult
import java.util.Queue

/**
 * Convenience class for sending requests to a single-threaded state machine
 * controlling access to a remote resource that does not support transactions.
 */
class RequestClient(private val requestQueue: Queue<RemoteRequest>) {
    /**
     * Send a single non-transactional read request to a remote resource.
     * Reads run in parallel.
     */
    fun readOne(query: String) = runRequest(OneRead(query))

    /**
     * Send a single non-transactional,exclusive write request to a remote
     * resource.
     * Writes run serially.
     */
    fun writeOne(query: String) = runRequest(OneWrite(query))

    /** Starts an exclusive session with the remote resource. */
    fun inExclusiveAccess(expectedUnits: Int) = ExclusiveAccess(expectedUnits)

    inner class ExclusiveAccess(expectedUnits: Int) :
        Transactionish<String, Unit> {
        private val uow = UnitOfWork(expectedUnits)

        /**
         * Sends a single read request to a remote resource.
         * These may run in parallel with other outstanding read requests.
         */
        override fun readOne(query: String) = runRequest(uow.readOne(query))

        /**
         * Send a single exclusive write request to a remote resource in a
         * transactional context.
         * Writes run serially.
         */
        override fun writeOne(query: String) = runRequest(uow.writeOne(query))

        /**
         * Cancels the current transaction context.
         * Other simple reads/writes or units of work may proceed afterward.
         */
        override fun cancel() {
            requestQueue.offer(uow.cancel())
        }

        /**
         * Aborts the current transaction context with instructions to the
         * remote resource for undoing changes.
         * Other simple reads/writes or units of work may proceed afterward.
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

private fun handleFailure(result: FailureRemoteResult): Nothing =
    error("TODO: Implement an exception scheme: $result")
