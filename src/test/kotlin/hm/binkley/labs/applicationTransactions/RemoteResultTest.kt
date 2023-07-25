package hm.binkley.labs.applicationTransactions

import io.kotest.matchers.shouldBe
import org.junit.jupiter.api.Test

internal class RemoteResultTest {
    @Test
    fun `should succeed`() {
        val success = SuccessRemoteResult(200, "GET NAME", "GET NAME: CHARLIE")

        success.status shouldBe 200
        success.query shouldBe "GET NAME"
        success.response shouldBe "GET NAME: CHARLIE"
    }

    @Test
    fun `should fail`() {
        val failure = FailureRemoteResult(400, "ABCD PQRSTUV", "YOU MESSED UP")

        failure.status shouldBe 400
        failure.query shouldBe "ABCD PQRSTUV"
        failure.errorMessage shouldBe "YOU MESSED UP"
    }

    @Test
    fun `should be busy`() {
        val failure = FailureRemoteResult(429, "GET NAME", "TRY AGAIN LATER")

        failure.isBusy() shouldBe true
    }
}
