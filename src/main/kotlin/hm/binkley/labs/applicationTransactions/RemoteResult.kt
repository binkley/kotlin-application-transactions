package hm.binkley.labs.applicationTransactions

/**
 * Assumption: the remote resource has an HTTP-like response with a status
 * code.
 * This could be restructured to support exceptions or other schemes for
 * detecting remote failures.
 */
sealed interface RemoteResult {
    val status: Int
    val query: String
}

data class SuccessRemoteResult(
    override val status: Int,
    override val query: String,
    val response: String,
) : RemoteResult

data class FailureRemoteResult(
    override val status: Int,
    override val query: String,
    val errorMessage: String,
) : RemoteResult {
    /**
     * See [_429 Too Many
     * Requests_](https://developer.mozilla.org/en-US/docs/Web/HTTP/Status/429).
     */
    fun isBusy() = 429 == status
}
