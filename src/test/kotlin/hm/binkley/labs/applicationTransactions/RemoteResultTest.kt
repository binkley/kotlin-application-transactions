package hm.binkley.labs.applicationTransactions

import io.kotest.matchers.shouldBe
import org.junit.jupiter.api.Test

internal class RemoteResultTest {
    @Test
    fun `should succeed`() {
        val success = SuccessRemoteResult(200, "DATA YOU WANT")

        success.status shouldBe 200
        success.response shouldBe "DATA YOU WANT"
    }

    @Test
    fun `should fail`() {
        val failure = FailureRemoteResult(500, "I MESSED UP")

        failure.status shouldBe 500
        failure.errorMessage shouldBe "I MESSED UP"
    }

    @Test
    fun `should be busy`() {
        val failure = FailureRemoteResult(429, "TRY AGAIN LATER")

        failure.isBusyRemoteResource() shouldBe true
    }
}
