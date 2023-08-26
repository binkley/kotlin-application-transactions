package hm.binkley.labs.applicationTransactions.processing

import hm.binkley.labs.applicationTransactions.FailureRemoteResult
import hm.binkley.labs.applicationTransactions.RemoteResult
import hm.binkley.labs.applicationTransactions.SuccessRemoteResult
import java.util.concurrent.TimeUnit.SECONDS

/**
 * An implementation of [RemoteResource] providing _policy_ for retrying
 * a "true remote resource" when it is busy assuming a constant pause between
 * retries (see [waitBetweenRemoteRetriesInSeconds].
 *
 * The retry policy is simple:
 * 1. Try the first attempt (which succeeds most times).
 * 2. If the remote resource is busy, try again up to [maxTries].
 */
class RemoteResourceWithBusyRetry(
    private val trueRemoteResource: RemoteResource,
    /**
     * How many times to try the remote resource before failing when it is busy.
     * The default is to try twice.
     */
    private val maxTries: Int = 2,
    /**
     * How long to wait for the remote resource to become idle.
     * The default is to wait 1 second.
     */
    private val waitBetweenRemoteRetriesInSeconds: Long = 1L,
) : RemoteResource {
    init {
        require(0 < maxTries) {
            "BUG: Calling the remote resource 0 times"
        }
    }

    /**
     * Retry a busy remote resource, pausing
     * [waitBetweenRemoteRetriesInSeconds] seconds between retries.
     */
    override fun call(query: String): RemoteResult {
        var tries = 1
        var response = trueRemoteResource.call(query)

        when (response) {
            is SuccessRemoteResult -> return response
            is FailureRemoteResult ->
                if (!response.isBusy()) return response
        }

        while (tries++ < maxTries) {
            // The remote resource was busy and failed previously
            SECONDS.sleep(waitBetweenRemoteRetriesInSeconds)
            response = trueRemoteResource.call(query)

            when (response) {
                is SuccessRemoteResult -> return response
                is FailureRemoteResult ->
                    if (!response.isBusy()) return response
            }
        }

        return response
    }
}
