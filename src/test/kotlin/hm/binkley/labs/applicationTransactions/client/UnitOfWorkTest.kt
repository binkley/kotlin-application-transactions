package hm.binkley.labs.applicationTransactions.client

import io.kotest.assertions.throwables.shouldThrow
import io.kotest.matchers.should
import io.kotest.matchers.shouldBe
import io.kotest.matchers.types.beInstanceOf
import org.junit.jupiter.api.Test
import java.util.concurrent.Future

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
        read.result should beInstanceOf<Future<RemoteResult>>()
    }

    @Test
    fun `should send a write query`() {
        val write = unitOfWork.write("GIVE BOB 10 QUID")

        write.id shouldBe unitOfWork.id
        write.expectedUnits shouldBe unitOfWork.expectedUnits
        write.currentUnit shouldBe 1
        write.query shouldBe "GIVE BOB 10 QUID"
        write.result should beInstanceOf<Future<RemoteResult>>()
    }

    @Test
    fun `should cancel (abandon) the unit of work`() {
        val rollback = unitOfWork.cancel()

        rollback should beInstanceOf<AbandonUnitOfWork>()
        rollback.id shouldBe unitOfWork.id
        rollback.undo shouldBe emptyList()
    }

    @Test
    fun `should abort (abandon) the unit of work with a list`() {
        val rollback = unitOfWork.abort(listOf("UNDO MY CHANGES"))

        rollback should beInstanceOf<AbandonUnitOfWork>()
        rollback.id shouldBe unitOfWork.id
        rollback.undo shouldBe listOf("UNDO MY CHANGES")
    }

    @Test
    fun `should abort (abandon) the unit of work with varargs`() {
        val rollback = unitOfWork.abort("UNDO MY CHANGES")

        rollback should beInstanceOf<AbandonUnitOfWork>()
        rollback.id shouldBe unitOfWork.id
        rollback.undo shouldBe listOf("UNDO MY CHANGES")
    }

    @Test
    fun `should close normally`() {
        unitOfWork.read("MY BIRTHDAY")
        unitOfWork.write("MY WEIGHT")
        unitOfWork.read("MY WEIGHT")

        unitOfWork.close()
    }

    @Test
    fun `should throw a bug if not enough work units to close`() {
        shouldThrow<IllegalStateException> {
            unitOfWork.close()
        }
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
