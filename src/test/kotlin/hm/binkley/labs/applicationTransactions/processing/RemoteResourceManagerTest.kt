package hm.binkley.labs.applicationTransactions.processing

import hm.binkley.labs.applicationTransactions.FailureRemoteResult
import hm.binkley.labs.applicationTransactions.RemoteResult
import hm.binkley.labs.applicationTransactions.SuccessRemoteResult
import io.kotest.matchers.shouldBe
import org.junit.jupiter.api.Test

internal class RemoteResourceManagerTest {
    @Test
    fun `should succeed on first try`() {
        val remoteResource = TestRecordingRemoteResource { _ ->
            SuccessRemoteResult(200, "READ NAME", "CHARLIE")
        }
        val manager = RemoteResourceManager(remoteResource)

        val result = manager.callWithBusyRetry("READ NAME")

        (result as SuccessRemoteResult).response shouldBe "CHARLIE"
        remoteResource.calls shouldBe listOf("READ NAME")
    }

    @Test
    fun `should fail on first try`() {
        val remoteResource = TestRecordingRemoteResource { query ->
            FailureRemoteResult(400, query, "SYNTAX ERROR: $query")
        }
        val manager = RemoteResourceManager(remoteResource)

        val result = manager.callWithBusyRetry("ABCD PQRSTUV")

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
        val manager = RemoteResourceManager(remoteResource)

        val result = manager.callWithBusyRetry("READ NAME")

        (result as SuccessRemoteResult).response shouldBe "CHARLIE"
        remoteResource.calls shouldBe listOf("READ NAME", "READ NAME")
    }

    @Test
    fun `should retry only once when remote stays busy`() {
        val remoteResource = TestRecordingRemoteResource {
            FailureRemoteResult(429, "SOME DATA", "TRY AGAIN")
        }
        val manager = RemoteResourceManager(remoteResource)

        val result = manager.callWithBusyRetry("READ NAME")

        (result as FailureRemoteResult).isBusy() shouldBe true
        remoteResource.calls shouldBe listOf("READ NAME", "READ NAME")
    }
}
