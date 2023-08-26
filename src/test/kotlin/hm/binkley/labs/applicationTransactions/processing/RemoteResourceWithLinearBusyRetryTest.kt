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
        val remoteResource = TestRecordingRemoteResource { _ ->
            SuccessRemoteResult(200, "READ NAME", "CHARLIE")
        }
        val withRetry = RemoteResourceWithLinearBusyRetry(remoteResource)

        val result = withRetry.call("READ NAME")

        (result as SuccessRemoteResult).response shouldBe "CHARLIE"
        remoteResource.calls shouldBe listOf("READ NAME")
    }

    @Test
    fun `should fail on first try`() {
        val remoteResource = TestRecordingRemoteResource { query ->
            FailureRemoteResult(400, query, "SYNTAX ERROR: $query")
        }
        val withRetry = RemoteResourceWithLinearBusyRetry(remoteResource)

        val result = withRetry.call("ABCD PQRSTUV")

        (result as FailureRemoteResult).errorMessage shouldBe
            "SYNTAX ERROR: ABCD PQRSTUV"
        remoteResource.calls shouldBe listOf("ABCD PQRSTUV")
    }

    @Test
    fun `should retry when busy`() {
        val realRemote = object : RemoteResource {
            private var busy = true
            override fun call(query: String): RemoteResult {
                return if (busy) {
                    busy = false
                    FailureRemoteResult(429, query, "TRY AGAIN")
                } else {
                    SuccessRemoteResult(200, query, "CHARLIE")
                }
            }
        }
        val remoteResource = TestRecordingRemoteResource(realRemote)
        val withRetry = RemoteResourceWithLinearBusyRetry(remoteResource)

        val result = withRetry.call("READ NAME")

        (result as SuccessRemoteResult).response shouldBe "CHARLIE"
        remoteResource.calls shouldBe listOf("READ NAME", "READ NAME")
    }

    @Test
    fun `should retry only once when remote stays busy`() {
        val remoteResource = TestRecordingRemoteResource {
            FailureRemoteResult(429, "SOME DATA", "TRY AGAIN")
        }
        val withRetry = RemoteResourceWithLinearBusyRetry(remoteResource)

        val result = withRetry.call("READ NAME")

        (result as FailureRemoteResult).isBusy() shouldBe true
        remoteResource.calls shouldBe listOf("READ NAME", "READ NAME")
    }

    @Test
    fun `should retry twice when remote stays busy if configured`() {
        val remoteResource = TestRecordingRemoteResource {
            FailureRemoteResult(429, "SOME DATA", "TRY AGAIN")
        }
        val withRetry = RemoteResourceWithLinearBusyRetry(
            trueRemoteResource = remoteResource,
            maxTries = 3
        )

        val result = withRetry.call("READ NAME")

        (result as FailureRemoteResult).isBusy() shouldBe true
        remoteResource.calls shouldBe
            listOf("READ NAME", "READ NAME", "READ NAME")
    }
}
