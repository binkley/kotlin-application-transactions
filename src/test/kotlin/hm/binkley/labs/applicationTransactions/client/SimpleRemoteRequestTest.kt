package hm.binkley.labs.applicationTransactions.client

import io.kotest.matchers.shouldBe
import org.junit.jupiter.api.Test

internal class SimpleRemoteRequestTest {
    @Test
    fun `should create one read request`() {
        val read = OneRead("WHAT COLOR IS THE SKY?")

        read.query shouldBe "WHAT COLOR IS THE SKY?"
    }

    @Test
    fun `should create one write request`() {
        val write = OneWrite("TURN THE SKY CLEAR FOR NIGHT")

        write.query shouldBe "TURN THE SKY CLEAR FOR NIGHT"
    }
}
