package hm.binkley.labs.applicationTransactions.processing

import hm.binkley.labs.applicationTransactions.RemoteResult

/**
 * A mock interface representing the remote resource which needs custom
 * handling for transactions.
 */
fun interface RemoteResource {
    fun call(query: String): RemoteResult
}
