package hm.binkley.labs.applicationTransactions.processing

import hm.binkley.labs.applicationTransactions.FailureRemoteResult
import hm.binkley.labs.applicationTransactions.RemoteResult
import hm.binkley.labs.applicationTransactions.SuccessRemoteResult
import java.util.concurrent.TimeUnit.SECONDS

/**
 * An adapter over a [RemoteResource] providing _policy_ for retrying the true
 * resource when it is busy.
 */
class RemoteResourceWithBusyRetry(
    private val trueRemoteResource: RemoteResource,
    /** How long to wait for the remote resource to become idle. */
    private val waitBeforeRetryRemoteInSeconds: Long = 1L,
) : RemoteResource {
    /**
     * Retry a busy remote resource exactly once, pausing
     * [waitBeforeRetryRemoteInSeconds] seconds before retrying.
     */
    override fun call(
        query: String,
    ): RemoteResult {
        when (val response = trueRemoteResource.call(query)) {
            is SuccessRemoteResult -> return response
            is FailureRemoteResult -> when {
                !response.isBusy() -> return response
            }
        }

        // Retry remote after waiting
        SECONDS.sleep(waitBeforeRetryRemoteInSeconds)

        return trueRemoteResource.call(query)
    }
}
