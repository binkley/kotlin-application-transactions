package hm.binkley.labs.applicationTransactions.client

import hm.binkley.labs.applicationTransactions.FailureRemoteResult
import hm.binkley.labs.applicationTransactions.OneRead
import hm.binkley.labs.applicationTransactions.OneWrite
import hm.binkley.labs.applicationTransactions.RemoteQuery
import hm.binkley.labs.applicationTransactions.RemoteRequest
import hm.binkley.labs.applicationTransactions.SuccessRemoteResult
import java.util.Queue

class RequestClient(private val requestQueue: Queue<RemoteRequest>) {
    fun readOne(query: String) = runRequest(OneRead(query))

    fun writeOne(query: String) = runRequest(OneWrite(query))

    fun inTransaction(expectedUnits: Int) = Transaction(expectedUnits)

    inner class Transaction(expectedUnits: Int) : Transactional<String, Unit> {
        private val uow = UnitOfWork(expectedUnits)

        override fun readOne(query: String) = runRequest(uow.readOne(query))

        override fun writeOne(query: String) = runRequest(uow.writeOne(query))

        override fun cancel() {
            requestQueue.offer(uow.cancel())
        }

        override fun abort(undo: List<String>) {
            requestQueue.offer(uow.abort(undo))
        }

        override fun close() = uow.close()
    }

    private fun runRequest(request: RemoteQuery): String {
        requestQueue.offer(request)
        return when (val result = request.result.get()) { // Blocking
            is SuccessRemoteResult -> result.response
            is FailureRemoteResult -> handleFailure(result)
        }
    }
}

private fun handleFailure(result: FailureRemoteResult): Nothing =
    error("TODO: Implement an exception scheme: $result")
