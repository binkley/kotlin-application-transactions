package hm.binkley.labs.applicationTransactions.processing

import io.kotest.assertions.throwables.shouldThrow
import io.kotest.matchers.shouldBe
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.Timeout
import java.util.concurrent.CompletableFuture
import java.util.concurrent.Executors.newSingleThreadExecutor
import java.util.concurrent.TimeUnit.DAYS
import java.util.concurrent.TimeUnit.MILLISECONDS
import java.util.concurrent.TimeUnit.SECONDS
import java.util.concurrent.TimeoutException

@Timeout(value = 1, unit = SECONDS)
internal class WorkerPoolTest {
    private val workers = WorkerPool(newSingleThreadExecutor())

    @AfterEach
    fun cleanup() = workers.close()

    @Test
    fun `should start with no work`() {
        workers.areAllDone() shouldBe true
        workers.waitForCompletion(1_000_000L, DAYS) // Test timeout
    }

    @Test
    fun `should have result read before checking the pool`() {
        val task = workers.submit { }
        task.get()

        workers.areAllDone() shouldBe true
        workers.waitForCompletion(1_000_000L, DAYS) // Test timeout
    }

    @Test
    @Timeout(value = 1L, unit = DAYS)
    fun `should wait on worker to complete`() {
        val realResult = CompletableFuture<Int>()
        val workerTask = workers.submit { realResult.get() }

        workers.areAllDone() shouldBe false
        shouldThrow<TimeoutException> {
            workers.waitForCompletion(1L, MILLISECONDS)
        }

        realResult.complete(42)
        workerTask.get() shouldBe 42

        workers.areAllDone() shouldBe true
        workers.waitForCompletion(1_000_000L, DAYS) // Test timeout
    }
}
