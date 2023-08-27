package hm.binkley.labs.applicationTransactions.processing

import hm.binkley.labs.applicationTransactions.FailureRemoteResult
import hm.binkley.labs.applicationTransactions.RemoteResult
import hm.binkley.labs.applicationTransactions.SuccessRemoteResult
import io.kotest.assertions.throwables.shouldThrow
import io.kotest.matchers.shouldBe
import org.junit.jupiter.api.Test

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
    fun `should succeed on first try`() {
        val remoteResource = recordingRemoteResourceAlwaysSucceeds()
        val withRetry = RemoteResourceWithLinearBusyRetry(remoteResource)
        val query = "READ NAME"

        val result = withRetry.call(query)

        remoteResource.calls shouldBe listOf(query)
        (result as SuccessRemoteResult).response shouldBe "CHARLIE"
    }

    @Test
    fun `should fail on first try`() {
        val remoteResource = recordingRemoteResourceAlwaysErrors()
        val withRetry = RemoteResourceWithLinearBusyRetry(remoteResource)
        val query = "ABCD PQRSTUV"

        val result = withRetry.call(query)

        remoteResource.calls shouldBe listOf(query)
        (result as FailureRemoteResult).errorMessage shouldBe
            "BAD SYNTAX: $query"
    }

    @Test
    fun `should retry when busy and then succeed`() {
        val remoteResource = recordingRemoteResourceBusyThenFree()
        val withRetry = RemoteResourceWithLinearBusyRetry(remoteResource)
        val query = "READ NAME"

        val result = withRetry.call(query)

        remoteResource.calls shouldBe listOf(query, query)
        (result as SuccessRemoteResult).response shouldBe "CHARLIE"
    }

    @Test
    fun `should retry while remote is busy and still fail`() {
        val remoteResource = recordingRemoteResourceAlwaysBusy()
        val withRetry = RemoteResourceWithLinearBusyRetry(remoteResource)
        val query = "READ NAME"

        val result = withRetry.call(query)

        remoteResource.calls shouldBe listOf(query, query)
        (result as FailureRemoteResult).isBusy() shouldBe true
    }

    @Test
    fun `should retry several times when remote stays busy if configured`() {
        val remoteResource = recordingRemoteResourceAlwaysBusy()
        val withRetry = RemoteResourceWithLinearBusyRetry(
            trueRemoteResource = remoteResource,
            maxTries = 3
        )
        val query = "READ NAME"

        val result = withRetry.call(query)

        remoteResource.calls shouldBe listOf(query, query, query)
        (result as FailureRemoteResult).isBusy() shouldBe true
    }
}

private fun recordingRemoteResourceAlwaysSucceeds() =
    TestRecordingRemoteResource { query ->
        SuccessRemoteResult(200, query, "CHARLIE")
    }

private fun recordingRemoteResourceAlwaysErrors() =
    TestRecordingRemoteResource { query ->
        FailureRemoteResult(400, query, "BAD SYNTAX: $query")
    }

private fun recordingRemoteResourceAlwaysBusy() =
    TestRecordingRemoteResource { query ->
        FailureRemoteResult(429, query, "TRY AGAIN")
    }

private fun recordingRemoteResourceBusyThenFree() =
    TestRecordingRemoteResource(object : RemoteResource {
        private var busy = true
        override fun call(query: String): RemoteResult {
            return if (busy) {
                busy = false
                FailureRemoteResult(429, query, "TRY AGAIN")
            } else {
                SuccessRemoteResult(200, query, "CHARLIE")
            }
        }
    })
