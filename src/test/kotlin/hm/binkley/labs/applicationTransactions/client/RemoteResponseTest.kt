package hm.binkley.labs.applicationTransactions.client

import io.kotest.matchers.shouldBe
import org.junit.jupiter.api.Test

internal class RemoteResponseTest {
    @Test
    fun `should succeed`() {
        val success = SuccessRemoteResponse(200, "DATA YOU WANT")

        success.status shouldBe 200
        success.response shouldBe "DATA YOU WANT"
    }

    @Test
    fun `should fail`() {
        val failure = FailureRemoteResponse(429, "TRY AGAIN LATER")

        failure.status shouldBe 429
        failure.errorMessage shouldBe "TRY AGAIN LATER"
    }
}
