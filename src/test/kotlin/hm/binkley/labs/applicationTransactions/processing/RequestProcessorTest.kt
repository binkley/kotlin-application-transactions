package hm.binkley.labs.applicationTransactions.processing

import hm.binkley.labs.applicationTransactions.OneRead
import hm.binkley.labs.applicationTransactions.OneWrite
import hm.binkley.labs.applicationTransactions.RemoteRequest
import hm.binkley.labs.applicationTransactions.SuccessRemoteResult
import io.kotest.matchers.should
import io.kotest.matchers.shouldBe
import io.kotest.matchers.types.beInstanceOf
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.Timeout
import java.util.concurrent.ConcurrentLinkedQueue
import java.util.concurrent.Executors.newCachedThreadPool
import java.util.concurrent.TimeUnit.SECONDS

/** Not typical unit test style; threads are challenging. */
@Timeout(value = 1L, unit = SECONDS) // Tests use threads
internal class RequestProcessorTest {
    @Test
    fun `should parallelize simple reads`() {
        val requestQueue = ConcurrentLinkedQueue<RemoteRequest>()
        val remoteResource = RemoteResource { query: String ->
            SuccessRemoteResult(200, "$query: BOB")
        }
        val threadPool = newCachedThreadPool()
        threadPool.submit(
            RequestProcessor(
                requestQueue,
                remoteResource,
                threadPool,
            )
        )

        val request = OneRead("READ NAME")
        requestQueue.offer(request)

        val result = request.result.get()
        result should beInstanceOf<SuccessRemoteResult>()
        (result as SuccessRemoteResult).response shouldBe "READ NAME: BOB"
    }

    @Test
    fun `should serialize simple writes`() {
        val requestQueue = ConcurrentLinkedQueue<RemoteRequest>()
        val remoteResource = RemoteResource { query: String ->
            SuccessRemoteResult(201, "$query: CHARLIE")
        }
        val threadPool = newCachedThreadPool()
        threadPool.submit(
            RequestProcessor(
                requestQueue,
                remoteResource,
                threadPool,
            )
        )

        val request = OneWrite("WRITE NAME")
        requestQueue.offer(request)

        val result = request.result.get()
        result should beInstanceOf<SuccessRemoteResult>()
        (result as SuccessRemoteResult).response shouldBe "WRITE NAME: CHARLIE"
    }
}
