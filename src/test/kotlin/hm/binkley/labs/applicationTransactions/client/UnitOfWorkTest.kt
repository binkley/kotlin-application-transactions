package hm.binkley.labs.applicationTransactions.client

import io.kotest.assertions.throwables.shouldThrow
import io.kotest.matchers.should
import io.kotest.matchers.shouldBe
import io.kotest.matchers.types.beInstanceOf
import org.junit.jupiter.api.Test

class UnitOfWorkTest {
    private val unitOfWork = UnitOfWork(3)

    @Test
    fun `should expose total units expected`() {
        unitOfWork.expectedUnits shouldBe 3
    }

    @Test
    fun `should send a read query`() {
        val read = unitOfWork.read("ASK BOB HIS NAME")

        read.id shouldBe unitOfWork.id
        read.expectedUnits shouldBe unitOfWork.expectedUnits
        read.currentUnit shouldBe 1
        read.query shouldBe "ASK BOB HIS NAME"
    }

    @Test
    fun `should send a write query`() {
        val write = unitOfWork.write("GIVE BOB 10 QUID")

        write.id shouldBe unitOfWork.id
        write.expectedUnits shouldBe unitOfWork.expectedUnits
        write.currentUnit shouldBe 1
        write.query shouldBe "GIVE BOB 10 QUID"
    }

    @Test
    fun `should rollback (abandon) the unit of work`() {
        val rollback = unitOfWork.rollback()

        rollback should beInstanceOf<AbandonUnitOfWork>()
        rollback.id shouldBe unitOfWork.id
    }

    @Test
    fun `should throw a bug if more work units than expected for a read`() {
        unitOfWork.read("BOB")
        unitOfWork.read("NANCY")
        unitOfWork.read("ALICE")

        shouldThrow<IllegalStateException> {
            unitOfWork.read("POOR CHARLIE")
        }
    }

    @Test
    fun `should throw a bug if more work units than expected for a write`() {
        unitOfWork.read("BOB")
        unitOfWork.read("NANCY")
        unitOfWork.read("ALICE")

        shouldThrow<IllegalStateException> {
            unitOfWork.write("POOR CHARLIE")
        }
    }
}
