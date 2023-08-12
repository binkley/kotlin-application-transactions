package hm.binkley.labs.applicationTransactions

import io.kotest.matchers.should
import io.kotest.matchers.shouldBe
import io.kotest.matchers.types.beInstanceOf
import org.junit.jupiter.api.Test
import java.util.UUID
import java.util.concurrent.Future

/**
 * Tests only single requests, not work units.
 *
 * @see UnitOfWorkTest
 */
internal class RemoteRequestTest {
    @Test
    fun `should create one read request`() {
        val read = OneRead("WHAT COLOR IS THE SKY?")

        read.query shouldBe "WHAT COLOR IS THE SKY?"
        read.result should beInstanceOf<Future<RemoteResult>>()
    }

    @Test
    fun `should create one write request`() {
        val write = WriteWorkUnit(
            id = UUID.randomUUID(),
            expectedUnits = 1,
            currentUnit = 1,
            query = "TURN THE SKY CLEAR FOR NIGHT"
        )

        write.query shouldBe "TURN THE SKY CLEAR FOR NIGHT"
        write.result should beInstanceOf<Future<RemoteResult>>()
    }
}
