package hm.binkley.labs.applicationTransactions.client

import java.util.Queue

class Client(private val requestQueue: Queue<RemoteRequest>) {
    fun readOne(query: String) = runRequest(OneRead(query))

    fun writeOne(query: String) = runRequest(OneWrite(query))

    fun inTransaction(expectedUnits: Int) = Transaction(expectedUnits)

    inner class Transaction(expectedUnits: Int) : AutoCloseable {
        private val uow = UnitOfWork(expectedUnits)

        fun readOne(query: String) = runRequest(uow.readOne(query))

        fun writeOne(query: String) = runRequest(uow.writeOne(query))

        fun cancel() {
            requestQueue.offer(uow.cancel())
        }

        fun abort(undo: List<String>) {
            requestQueue.offer(uow.abort(undo))
        }

        fun abort(vararg undo: String) = abort(undo.asList())

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
