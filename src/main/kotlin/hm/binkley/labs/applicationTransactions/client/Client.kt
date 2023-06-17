package hm.binkley.labs.applicationTransactions.client

import java.util.Queue
import java.util.concurrent.CompletableFuture

class Client(private val requestQueue: Queue<RemoteRequest>) {
    fun read(query: String): String {
        val completion = CompletableFuture<RemoteResult>()
        requestQueue.offer(OneRead(query, completion))
        return when (val result = completion.get()) { // Blocking
            is SuccessRemoteResult -> result.response
            is FailureRemoteResult -> handleFailure(result)
        }
    }

    fun write(query: String): String {
        val completion = CompletableFuture<RemoteResult>()
        requestQueue.offer(OneWrite(query, completion))
        return when (val result = completion.get()) { // Blocking
            is SuccessRemoteResult -> result.response
            is FailureRemoteResult -> handleFailure(result)
        }
    }
}

private fun handleFailure(result: FailureRemoteResult): Nothing {
    error("TODO: Implement an exception scheme: $result")
}
