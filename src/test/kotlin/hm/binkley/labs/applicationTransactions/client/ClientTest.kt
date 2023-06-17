package hm.binkley.labs.applicationTransactions.client

import io.kotest.assertions.throwables.shouldThrow
import io.kotest.matchers.shouldBe
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.Timeout
import java.util.concurrent.ConcurrentLinkedQueue

internal class ClientTest {
    private val requestQueue = ConcurrentLinkedQueue<RemoteRequest>()
    private val client = Client(requestQueue)

    @Test
    @Timeout(1000L) // Testing with threads should always do this
    fun `should succeed at one read`() {
        Thread {
            while (true) {
                when (val request = requestQueue.poll()) {
                    null -> continue
                    is OneRead -> request.result.complete(
                        SuccessRemoteResult(200, "GREEN")
                    )

                    else -> FailureRemoteResult(
                        -1, // There are no negative status codes
                        "Wrong request type: $request"
                    )
                }
            }
        }.start()

        val response = client.read("WHAT IS YOUR FAVORITE COLOR?")

        response shouldBe "GREEN"
    }

    @Test
    @Timeout(1000L) // Testing with threads should always do this
    fun `should fail at one read`() {
        Thread {
            while (true) {
                when (val request = requestQueue.poll()) {
                    null -> continue
                    is OneRead -> request.result.complete(
                        FailureRemoteResult(429, "SLOW DOWN, PARDNER")
                    )

                    else -> FailureRemoteResult(
                        -1, // There are no negative status codes
                        "Wrong request type: $request"
                    )
                }
            }
        }.start()

        shouldThrow<IllegalStateException> {
            client.read("WHAT IS YOUR FAVORITE COLOR?")
        }
    }

    @Test
    @Timeout(1000L) // Testing with threads should always do this
    fun `should succeed at one write`() {
        Thread {
            while (true) {
                when (val request = requestQueue.poll()) {
                    null -> continue
                    is OneWrite -> request.result.complete(
                        SuccessRemoteResult(200, "BLUE IS THE NEW GREEN")
                    )

                    else -> FailureRemoteResult(
                        -1, // There are no negative status codes
                        "Wrong request type: $request"
                    )
                }
            }
        }.start()

        val response = client.write("CHANGE COLORS")

        response shouldBe "BLUE IS THE NEW GREEN"
    }

    @Test
    @Timeout(1000L) // Testing with threads should always do this
    fun `should fail at one write`() {
        Thread {
            while (true) {
                when (val request = requestQueue.poll()) {
                    null -> continue
                    is OneWrite -> request.result.complete(
                        FailureRemoteResult(429, "SLOW DOWN, PARDNER")
                    )

                    else -> FailureRemoteResult(
                        -1, // There are no negative status codes
                        "Wrong request type: $request"
                    )
                }
            }
        }.start()

        shouldThrow<IllegalStateException> {
            client.write("WHAT IS YOUR FAVORITE COLOR?")
        }
    }
}
