package hm.binkley.labs.util

import io.kotest.matchers.collections.shouldHaveSize
import io.kotest.matchers.shouldBe
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.Timeout
import java.util.concurrent.ArrayBlockingQueue
import java.util.concurrent.TimeUnit.MILLISECONDS
import java.util.concurrent.TimeUnit.SECONDS

@Timeout(value = 2L, unit = SECONDS)
internal class SearchableBlockingQueueTest {
    private val shared = ArrayBlockingQueue<String>(3)
    private val queue = SearchableBlockingQueue<String>(shared)

    @Test
    fun `should start with none`() {
        queue shouldHaveSize 0
    }

    @Test
    fun `should have one after offering one`() {
        val accepted = queue.offer("BOB")

        accepted shouldBe true
        queue shouldHaveSize 1
    }

    @Test
    fun `should fail to offer one after time elapses`() {
        for (n in 1..shared.remainingCapacity())
            queue.offer("BOB #$n")

        val accepted = queue.offer("NANCY", 1L, MILLISECONDS)

        accepted shouldBe false
        queue.remainingCapacity() shouldBe 0
    }

    @Test
    fun `should peek at first one after putting one`() {
        queue.put("BOB")

        queue.peek() shouldBe "BOB"
        queue.size shouldBe 1
    }

    @Test
    fun `should peek at first one after spillover`() {
        queue.put("BOB")

        queue.poll { it == "NANCY" }

        queue.peek() shouldBe "BOB"
        queue shouldHaveSize 1
    }

    @Test
    fun `should have one after putting one`() {
        queue.put("BOB")

        queue shouldHaveSize 1
    }

    @Test
    fun `should take one after putting one`() {
        queue.put("BOB")

        queue.take() shouldBe "BOB"
    }

    @Test
    fun `should take one from spillover`() {
        queue.put("BOB")

        queue.poll { it == "NANCY" }

        queue.take() shouldBe "BOB"
    }

    @Test
    fun `should poll none`() {
        queue.poll() shouldBe null
    }

    @Test
    fun `should poll none after time elapses`() {
        queue.poll(2L, MILLISECONDS) shouldBe null
    }

    @Test
    fun `should poll one after putting one`() {
        queue.put("BOB")

        queue.poll() shouldBe "BOB"
    }

    @Test
    fun `should poll one after spillover`() {
        queue.put("BOB")

        queue.poll { it == "NANCY" }

        queue.poll() shouldBe "BOB"
    }

    @Test
    fun `should poll one after putting one before time elapses`() {
        queue.put("BOB")

        queue.poll(2L, SECONDS) shouldBe "BOB"
    }

    @Test
    fun `should poll one after spillover before time elapses`() {
        queue.put("BOB")

        queue.poll { it == "NANCY" }

        queue.poll(2L, SECONDS) shouldBe "BOB"
    }

    @Test
    fun `should poll none matching after time elapses`() {
        val none = queue.poll { it == "NANCY" }

        none shouldBe null
    }

    @Test
    fun `should poll one matching`() {
        queue.put("BOB")
        queue.put("NANCY")

        val matched = queue.poll { it == "NANCY" }

        matched shouldBe "NANCY"
        queue shouldBe listOf("BOB")
    }

    @Test
    fun `should poll one matching with spillover`() {
        queue.put("BOB")
        queue.put("NANCY")

        queue.poll { it == "NANCY" }
        val matched = queue.poll { it == "BOB" }

        matched shouldBe "BOB"
        queue shouldBe emptyList()
    }

    @Test
    fun `should poll one matching and time elapses`() {
        val matched = queue.poll(1L, MILLISECONDS) { it == "NANCY" }

        matched shouldBe null
    }

    @Test
    fun `should iterate when empty`() {
        var hasElements = false

        for (e in queue) hasElements = true

        hasElements shouldBe false
    }

    @Test
    fun `should iterate when not empty`() {
        var hasElements = false

        queue.put("BOB")

        for (e in queue) hasElements = true

        hasElements shouldBe true
    }

    @Test
    fun `should iterate when spillover`() {
        var hasElements = false

        queue.put("BOB")
        queue.poll { it == "NANCY" }

        for (e in queue) hasElements = true

        hasElements shouldBe true
    }

    @Test
    fun `should remove from the iterator`() {
        queue.put("BOB")

        val itr = queue.iterator()

        itr.hasNext() shouldBe true
        itr.next() shouldBe "BOB"
        itr.remove()

        queue shouldBe emptyList()
    }

    @Test
    fun `should remove from the iterator with spillover`() {
        queue.put("BOB")
        queue.put("NANCY")

        queue.poll { it == "NANCY" }

        val itr = queue.iterator()

        itr.hasNext() shouldBe true
        itr.next() shouldBe "BOB"
        itr.remove()

        queue shouldBe emptyList()
    }

    @Test
    fun `should have the capacity of the shared queue`() {
        queue.remainingCapacity() shouldBe shared.remainingCapacity()
    }

    @Test
    fun `should have the capacity of the shared queue after spillover`() {
        queue.put("BOB")
        queue.put("NANCY")

        queue.poll { it == "NANCY" }

        queue.remainingCapacity() shouldBe shared.remainingCapacity()
    }

    @Test
    fun `should drain to a collection when there are no elements`() {
        val c = mutableListOf<String>()

        val drained = queue.drainTo(c)

        drained shouldBe c.size
        c shouldBe emptyList()
        queue shouldBe emptyList()
    }

    @Test
    fun `should drain to a collection when there is no spillover`() {
        val c = mutableListOf<String>()
        queue.put("BOB")

        val drained = queue.drainTo(c)

        drained shouldBe c.size
        c shouldBe listOf("BOB")
        queue shouldBe emptyList()
    }

    @Test
    fun `should drain to a collection when there is spillover`() {
        val c = mutableListOf<String>()
        queue.put("BOB")
        queue.put("NANCY")
        queue.poll { it == "NANCY" }

        val drained = queue.drainTo(c)

        drained shouldBe c.size
        c shouldBe listOf("BOB")
        queue shouldBe emptyList()
    }

    @Test
    fun `should drain to a limited collection when there is no spillover`() {
        val c = mutableListOf<String>()
        queue.put("BOB")
        queue.put("NANCY")

        val drained = queue.drainTo(c, 1)

        drained shouldBe c.size
        c shouldBe listOf("BOB")
        queue shouldBe listOf("NANCY")
    }

    @Test
    fun `should drain to a limited collection when spillover is enough`() {
        val c = mutableListOf<String>()
        queue.put("BOB")
        queue.put("NANCY")
        queue.poll { it == "NANCY" } // BOB goes to spillover
        queue.put("FRED")
        queue.put("BARNEY")
        queue.poll { it == "BARNEY" } // FRED goes to spillover

        val drained = queue.drainTo(c, 1)

        drained shouldBe c.size
        c shouldBe listOf("BOB")
        queue shouldBe listOf("FRED")
    }
}
