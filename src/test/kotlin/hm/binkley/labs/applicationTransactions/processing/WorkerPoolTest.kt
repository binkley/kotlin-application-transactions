package hm.binkley.labs.applicationTransactions.processing

import io.kotest.matchers.shouldBe
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.Timeout
import java.util.concurrent.CompletableFuture
import java.util.concurrent.Executors.newSingleThreadExecutor
import java.util.concurrent.TimeUnit.DAYS
import java.util.concurrent.TimeUnit.MILLISECONDS
import java.util.concurrent.TimeUnit.SECONDS

@Timeout(value = 1, unit = SECONDS)
internal class WorkerPoolTest {
    private val workers = WorkerPool(newSingleThreadExecutor())

    @AfterEach
    fun cleanup() = workers.close()

    @Test
    fun `should start with no work`() {
        workers.areAllDone() shouldBe true
        workers.awaitCompletion(1_000_000L, DAYS) shouldBe true
    }

    @Test
    fun `should have result read before checking the pool`() {
        val task = workers.submit { }
        task.get()

        workers.areAllDone() shouldBe true
        workers.awaitCompletion(1_000_000L, DAYS) shouldBe true
    }

    @Test
    @Timeout(value = 1L, unit = DAYS)
    fun `should wait on worker to complete`() {
        val realResult = CompletableFuture<Int>()
        val workerTask = workers.submit { realResult.get() }

        workers.areAllDone() shouldBe false
        workers.awaitCompletion(1L, MILLISECONDS) shouldBe false

        realResult.complete(42)
        workerTask.get() shouldBe 42

        workers.areAllDone() shouldBe true
        workers.awaitCompletion(1_000_000L, DAYS) shouldBe true
    }
}
