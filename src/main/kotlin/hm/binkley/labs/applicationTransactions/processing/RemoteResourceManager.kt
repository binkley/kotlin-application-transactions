package hm.binkley.labs.applicationTransactions.processing

import hm.binkley.labs.applicationTransactions.FailureRemoteResult
import hm.binkley.labs.applicationTransactions.RemoteResult
import hm.binkley.labs.applicationTransactions.SuccessRemoteResult
import java.util.concurrent.TimeUnit.SECONDS

/**
 * An adapter over a [RemoteResource] providing _policy_ for retrying the
 * resource when it is busy.
 * It does not provide access to [RemoteResource.call].
 *
 * The structure of this class will depend on implementation language.
 * With Kotlin it is nicest to put [remoteResource] last in the constructor
 * to take advantage of syntactic sugar.
 * Named parameters in function calls may come into advantage.
 */
class RemoteResourceManager(
    /** How long to wait for the remote resource to become idle. */
    private val awaitRemoteInSeconds: Long = 1L,
    private val remoteResource: RemoteResource,
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
