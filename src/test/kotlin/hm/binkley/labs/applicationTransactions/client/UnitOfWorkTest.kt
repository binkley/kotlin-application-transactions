package hm.binkley.labs.applicationTransactions.client

import hm.binkley.labs.applicationTransactions.AbandonUnitOfWork
import hm.binkley.labs.applicationTransactions.RemoteResult
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
        val read = unitOfWork.readOne("ASK BOB HIS NAME")

        read.id shouldBe unitOfWork.id
        read.expectedUnits shouldBe unitOfWork.expectedUnits
        read.currentUnit shouldBe 1
        read.query shouldBe "ASK BOB HIS NAME"
        read.result should beInstanceOf<Future<RemoteResult>>()
    }

    @Test
    fun `should send a write query`() {
        val write = unitOfWork.writeOne("GIVE BOB 10 QUID")

        write.id shouldBe unitOfWork.id
        write.expectedUnits shouldBe unitOfWork.expectedUnits
        write.currentUnit shouldBe 1
        write.query shouldBe "GIVE BOB 10 QUID"
        write.result should beInstanceOf<Future<RemoteResult>>()
    }

    @Test
    fun `should cancel (abandon) the unit of work`() {
        val rollback = unitOfWork.cancelAndKeepChanges()

        rollback should beInstanceOf<AbandonUnitOfWork>()
        rollback.id shouldBe unitOfWork.id
        rollback.expectedUnits shouldBe unitOfWork.expectedUnits
        // Abandon with intent jumps to the last unit so that "close" can detect
        // bugs in finishing early
        rollback.currentUnit shouldBe 3
        rollback.undo shouldBe emptyList()
    }

    @Test
    fun `should abort (abandon) the unit of work with a list`() {
        val rollback =
            unitOfWork.cancelAndUndoChanges(listOf("UNDO MY CHANGES"))

        rollback should beInstanceOf<AbandonUnitOfWork>()
        rollback.id shouldBe unitOfWork.id
        rollback.expectedUnits shouldBe unitOfWork.expectedUnits
        // Abandon with intent jumps to the last unit so that "close" can detect
        // bugs in finishing early
        rollback.currentUnit shouldBe 3
        rollback.undo shouldBe listOf("UNDO MY CHANGES")
    }

    @Test
    fun `should abort (abandon) the unit of work with varargs`() {
        val rollback = unitOfWork.cancelAndUndoChanges("UNDO MY CHANGES")

        rollback should beInstanceOf<AbandonUnitOfWork>()
        rollback.id shouldBe unitOfWork.id
        rollback.expectedUnits shouldBe unitOfWork.expectedUnits
        // Abandon with intent jumps to the last unit so that "close" can detect
        // bugs in finishing early
        rollback.currentUnit shouldBe 3
        rollback.undo shouldBe listOf("UNDO MY CHANGES")
    }

    @Test
    fun `should close normally`() {
        unitOfWork.readOne("MY BIRTHDAY")
        unitOfWork.writeOne("MY WEIGHT")
        unitOfWork.readOne("MY WEIGHT")

        unitOfWork.close()
    }

    @Test
    fun `should throw a bug when closing prematurely`() {
        shouldThrow<IllegalStateException> {
            unitOfWork.close()
        }
    }

    @Test
    fun `should throw a bug if more work units than expected for a read`() {
        unitOfWork.readOne("BOB")
        unitOfWork.readOne("NANCY")
        unitOfWork.readOne("ALICE")

        shouldThrow<IllegalStateException> {
            unitOfWork.readOne("POOR CHARLIE")
        }
    }

    @Test
    fun `should throw a bug if more work units than expected for a write`() {
        unitOfWork.readOne("BOB")
        unitOfWork.readOne("NANCY")
        unitOfWork.readOne("ALICE")

        shouldThrow<IllegalStateException> {
            unitOfWork.writeOne("POOR CHARLIE")
        }
    }

    @Test
    fun `should throw a bug if abort with no undo instructions`() {
        shouldThrow<IllegalArgumentException> {
            unitOfWork.cancelAndUndoChanges(emptyList())
        }
    }
}
