package hm.binkley.labs.applicationTransactions

sealed interface RemoteResult {
    val status: Int
}

data class SuccessRemoteResult(
    override val status: Int,
    val response: String,
) : RemoteResult

data class FailureRemoteResult(
    override val status: Int,
    val errorMessage: String,
) : RemoteResult
