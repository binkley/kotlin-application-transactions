package hm.binkley.labs.applicationTransactions.client

sealed interface RemoteResponse {
    val status: Int
}

data class SuccessRemoteResponse(
    override val status: Int,
    val response: String,
) : RemoteResponse

data class FailureRemoteResponse(
    override val status: Int,
    val errorMessage: String,
) : RemoteResponse
