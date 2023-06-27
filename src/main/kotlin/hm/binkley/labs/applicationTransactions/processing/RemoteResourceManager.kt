package hm.binkley.labs.applicationTransactions.processing

import hm.binkley.labs.applicationTransactions.FailureRemoteResult
import hm.binkley.labs.applicationTransactions.RemoteResult
import hm.binkley.labs.applicationTransactions.SuccessRemoteResult
import java.util.concurrent.TimeUnit.SECONDS

/**
 * An adapter over a [RemoteResource] providing _policy_ for retrying the
 * resource when it is busy.
 * It does not provide access to [RemoteResource.call].
 */
class RemoteResourceManager(
    private val remoteResource: RemoteResource,
    /** How long to wait for the remote resource to become idle. */
    private val awaitRemoteInSeconds: Long = 1L,
) {
    /**
     * Retry a busy remote resource exactly once, pausing [awaitRemoteInSeconds]
     * seconds before retrying.
     */
    fun callWithBusyRetry(
        query: String,
    ): RemoteResult {
        when (val response = remoteResource.call(query)) {
            is SuccessRemoteResult -> return response
            is FailureRemoteResult -> when {
                !response.isBusyRemoteResource() -> return response
            }
        }

        // Retry remote after waiting
        SECONDS.sleep(awaitRemoteInSeconds)

        return remoteResource.call(query)
    }
}
