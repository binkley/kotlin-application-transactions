package hm.binkley.labs.applicationTransactions.client

/** A sample exception type. */
class ExampleFrameworkException(
    /** The remote failure status code, or `0` if an internal failure. */
    val status: Int,
    /** The remote query, or `null` if an internal failure. */
    val query: String? = null,
    val errorMessage: String? = null,
) : Exception(exceptionMessage(status, query, errorMessage))

private fun exceptionMessage(
    status: Int,
    query: String?,
    errorMessage: String?
): String {
    val message = StringBuilder(errorMessage ?: "Unsure what happened")
    message.append("; remote status code: $status")

    if (null != query) message.append("; remote query: $query")

    return message.toString()
}
