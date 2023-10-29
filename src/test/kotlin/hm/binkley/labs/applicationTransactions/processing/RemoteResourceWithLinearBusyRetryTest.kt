package hm.binkley.labs.applicationTransactions.processing

import hm.binkley.labs.applicationTransactions.FailureRemoteResult
import hm.binkley.labs.applicationTransactions.RemoteResult
import hm.binkley.labs.applicationTransactions.SuccessRemoteResult
import io.kotest.assertions.throwables.shouldThrow
import io.kotest.matchers.shouldBe
import org.junit.jupiter.api.Test

private const val SUCCESS_RESULT_VALUE = "CHARLIE"
private const val BAD_SYNTAX_QUERY = "ABCD PQRSTUV"

internal class RemoteResourceWithLinearBusyRetryTest {
    @Test
    fun `should require at least 1 try to the remote resource`() {
        shouldThrow<IllegalArgumentException> {
            RemoteResourceWithLinearBusyRetry(
                trueRemoteResource = {
                    FailureRemoteResult(500, "FAIL", "CONSTRUCTOR FAILED")
                },
                maxTries = 0 // This is the problem
            )
        }
    }

    @Test
    fun `should default to 2 tries and 1 second between retries`() {
        val withRetry = RemoteResourceWithLinearBusyRetry(
            recordingRemoteResourceAlwaysSucceeds()
        )

        withRetry.maxTries shouldBe 2
        withRetry.waitBetweenRetriesInSeconds shouldBe 1
    }

    @Test
    fun `should succeed on first try as is expected in the usual case`() {
        val remoteResource = recordingRemoteResourceAlwaysSucceeds()
        val withRetry = RemoteResourceWithLinearBusyRetry(remoteResource)
        val query = "READ NAME"

        val result = withRetry.call(query)

        remoteResource.shouldHaveCalledExactlyInOrder(query)
        result.shouldBeSuccessResult()
    }

    @Test
    fun `should fail on first try even when configured for retry`() {
        val remoteResource = recordingRemoteResourceAlwaysErrors()
        val withRetry = RemoteResourceWithLinearBusyRetry(
            trueRemoteResource = remoteResource,
            maxTries = 2,
        )
        val query = BAD_SYNTAX_QUERY

        val result = withRetry.call(query)

        remoteResource.shouldHaveCalledExactlyInOrder(query)
        result.shouldBeBadSyntaxFailure()
    }

    @Test
    fun `should fail on first try then succeed on retry`() {
        val remoteResource = recordingRemoteResourceBusyThenSucceeds()
        val withRetry = RemoteResourceWithLinearBusyRetry(remoteResource)
        val query = "READ NAME"

        val result = withRetry.call(query)

        remoteResource.shouldHaveCalledExactlyInOrder(query, query)
        result.shouldBeSuccessResult()
    }

    @Test
    fun `should fail busy remote after one try`() {
        val remoteResource = recordingRemoteResourceAlwaysBusy()
        val withRetry = RemoteResourceWithLinearBusyRetry(
            trueRemoteResource = remoteResource,
            maxTries = 1
        )
        val query = "READ NAME"

        val result = withRetry.call(query)

        remoteResource.shouldHaveCalledExactlyInOrder(query)
        result.shouldBeBusyFailure()
    }

    @Test
    fun `should fail busy remote after two tries`() {
        val remoteResource = recordingRemoteResourceAlwaysBusy()
        val withRetry = RemoteResourceWithLinearBusyRetry(
            trueRemoteResource = remoteResource,
            maxTries = 2
        )
        val query = "READ NAME"

        val result = withRetry.call(query)

        remoteResource.shouldHaveCalledExactlyInOrder(query, query)
        result.shouldBeBusyFailure()
    }

    @Test
    fun `should fail busy remote after many tries`() {
        val remoteResource = recordingRemoteResourceAlwaysBusy()
        val withRetry = RemoteResourceWithLinearBusyRetry(
            trueRemoteResource = remoteResource,
            maxTries = 3
        )
        val query = "READ NAME"

        val result = withRetry.call(query)

        remoteResource.shouldHaveCalledExactlyInOrder(query, query, query)
        result.shouldBeBusyFailure()
    }

    @Test
    fun `should retry busy remote and get an error`() {
        val remoteResource = recordingRemoteResourceBusyThenError()
        val withRetry = RemoteResourceWithLinearBusyRetry(remoteResource)
        val query = BAD_SYNTAX_QUERY

        val result = withRetry.call(query)

        remoteResource.shouldHaveCalledExactlyInOrder(query, query)
        result.shouldBeBadSyntaxFailure()
    }
}

private fun recordingRemoteResourceAlwaysSucceeds() =
    TestRecordingRemoteResource { query ->
        SuccessRemoteResult(200, query, SUCCESS_RESULT_VALUE)
    }

private fun recordingRemoteResourceAlwaysErrors() =
    TestRecordingRemoteResource { query ->
        require(BAD_SYNTAX_QUERY == query)
        FailureRemoteResult(400, query, "BAD SYNTAX: $query")
    }

private fun recordingRemoteResourceAlwaysBusy() =
    TestRecordingRemoteResource { query ->
        FailureRemoteResult(429, query, "TRY AGAIN")
    }

private fun recordingRemoteResourceBusyThenSucceeds() =
    TestRecordingRemoteResource(object : RemoteResource {
        private var busy = true
        override fun call(query: String): RemoteResult {
            return if (busy) {
                busy = false
                FailureRemoteResult(429, query, "TRY AGAIN")
            } else {
                SuccessRemoteResult(200, query, SUCCESS_RESULT_VALUE)
            }
        }
    })

private fun recordingRemoteResourceBusyThenError() =
    TestRecordingRemoteResource(object : RemoteResource {
        private var busy = true
        override fun call(query: String): RemoteResult {
            return if (busy) {
                busy = false
                FailureRemoteResult(429, query, "TRY AGAIN")
            } else {
                FailureRemoteResult(400, query, "BAD SYNTAX: $query")
            }
        }
    })

private fun TestRecordingRemoteResource.shouldHaveCalledExactlyInOrder(
    vararg queries: String
) = this.calls shouldBe queries.toList()

private fun RemoteResult.shouldBeSuccessResult() =
    (this as SuccessRemoteResult).response shouldBe SUCCESS_RESULT_VALUE

private fun RemoteResult.shouldBeBadSyntaxFailure() =
    (this as FailureRemoteResult).errorMessage shouldBe
        "BAD SYNTAX: $BAD_SYNTAX_QUERY"

private fun RemoteResult.shouldBeBusyFailure() =
    (this as FailureRemoteResult).isBusy() shouldBe true
