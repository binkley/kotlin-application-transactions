package hm.binkley.labs.applicationTransactions.processing

import hm.binkley.labs.applicationTransactions.FailureRemoteResult
import hm.binkley.labs.applicationTransactions.OneRead
import hm.binkley.labs.applicationTransactions.OneWrite
import hm.binkley.labs.applicationTransactions.RemoteRequest
import hm.binkley.labs.applicationTransactions.SuccessRemoteResult
import hm.binkley.labs.applicationTransactions.client.UnitOfWork
import io.kotest.matchers.should
import io.kotest.matchers.shouldBe
import io.kotest.matchers.types.beInstanceOf
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.Timeout
import java.util.concurrent.ConcurrentLinkedQueue
import java.util.concurrent.Executors.newCachedThreadPool
import java.util.concurrent.TimeUnit.SECONDS

/** Not typical unit test style; threads are challenging. */
@Timeout(value = 1L, unit = SECONDS) // Tests use threads
internal class RequestProcessorTest {
    private val requestQueue = ConcurrentLinkedQueue<RemoteRequest>()
    private val threadPool = newCachedThreadPool()

    @AfterEach
    fun cleanup() = threadPool.shutdown()

    @Test
    fun `should process simple reads`() {
        runSuccessRequestProcessor()

        val request = OneRead("READ NAME")
        requestQueue.offer(request)

        val result = request.result.get()
        result should beInstanceOf<SuccessRemoteResult>()
        (result as SuccessRemoteResult).response shouldBe "READ NAME: CHARLIE"
    }

    @Test
    fun `should process simple writes`() {
        runSuccessRequestProcessor()

        val request = OneWrite("WRITE NAME")
        requestQueue.offer(request)

        val result = request.result.get()
        result should beInstanceOf<SuccessRemoteResult>()
        (result as SuccessRemoteResult).response shouldBe "WRITE NAME: CHARLIE"
    }

    @Test
    fun `should process work unit reads`() {
        runSuccessRequestProcessor()

        val unitOfWork = UnitOfWork(1)
        val request = unitOfWork.readOne("READ NAME")
        requestQueue.offer(request)

        val result = request.result.get()
        result should beInstanceOf<SuccessRemoteResult>()
        (result as SuccessRemoteResult).response shouldBe "READ NAME: CHARLIE"
    }

    @Test
    fun `should process work unit writes`() {
        runSuccessRequestProcessor()

        val unitOfWork = UnitOfWork(1)
        val request = unitOfWork.writeOne("WRITE NAME")
        requestQueue.offer(request)

        val result = request.result.get()
        result should beInstanceOf<SuccessRemoteResult>()
        (result as SuccessRemoteResult).response shouldBe "WRITE NAME: CHARLIE"
    }

    @Test
    @Timeout(value = 2L, unit = SECONDS)
    fun `should process work unit abandons`() {
        runSuccessRequestProcessor()

        val unitOfWork = UnitOfWork(2)
        val read = unitOfWork.readOne("FAVORITE COLOR")
        requestQueue.offer(read)
        read.result.get()
        val cancel = unitOfWork.cancel()
        requestQueue.offer(cancel)

        cancel.result.get() shouldBe true
    }

    private fun runSuccessRequestProcessor() {
        threadPool.submit(
            RequestProcessor(requestQueue, threadPool) { query: String ->
                SuccessRemoteResult(200, "$query: CHARLIE")
            }
        )
    }

    private fun runFailRequestProcessor() {
        threadPool.submit(
            RequestProcessor(requestQueue, threadPool) { query: String ->
                FailureRemoteResult(400, "SYNTAX ERROR: $query")
            }
        )
    }

    private fun runBusyRequestProcessor() {
        threadPool.submit(
            RequestProcessor(requestQueue, threadPool) { query: String ->
                FailureRemoteResult(429, "TRY AGAIN IN 1 SECOND: $query")
            }
        )
    }
}
