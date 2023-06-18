package hm.binkley.labs.applicationTransactions.client

import java.util.Queue

class Client(private val requestQueue: Queue<RemoteRequest>) {
    fun readOne(query: String): String {
        val request = OneRead(query)
        requestQueue.offer(request)
        return when (val result = request.result.get()) { // Blocking
            is SuccessRemoteResult -> result.response
            is FailureRemoteResult -> handleFailure(result)
        }
    }

    fun writeOne(query: String): String {
        val request = OneWrite(query)
        requestQueue.offer(request)
        return when (val result = request.result.get()) { // Blocking
            is SuccessRemoteResult -> result.response
            is FailureRemoteResult -> handleFailure(result)
        }
    }
}

private fun handleFailure(result: FailureRemoteResult): Nothing =
    error("TODO: Implement an exception scheme: $result")
