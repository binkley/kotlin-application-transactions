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
internal class ReaderThreadsTest {
    private val readerThreads = ReaderThreads(newSingleThreadExecutor())

    @AfterEach
    fun cleanup() = readerThreads.close()

    @Test
    fun `should start with no work`() {
        readerThreads.areAllDone() shouldBe true
        readerThreads.awaitCompletion(1_000_000L, DAYS) shouldBe true
    }

    @Test
    fun `should have result read before checking the pool`() {
        val task = readerThreads.submit { }
        task.get()

        readerThreads.areAllDone() shouldBe true
        readerThreads.awaitCompletion(1_000_000L, DAYS) shouldBe true
    }

    @Test
    @Timeout(value = 1L, unit = DAYS)
    fun `should wait on readers to complete`() {
        val realResult = CompletableFuture<Int>()
        val readerTask = readerThreads.submit { realResult.get() }

        readerThreads.areAllDone() shouldBe false
        readerThreads.awaitCompletion(1L, MILLISECONDS) shouldBe false

        realResult.complete(42)
        readerTask.get() shouldBe 42

        readerThreads.areAllDone() shouldBe true
        readerThreads.awaitCompletion(1_000_000L, DAYS) shouldBe true
    }
}
