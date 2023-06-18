package hm.binkley.labs.applicationTransactions.client

import io.kotest.assertions.throwables.shouldThrow
import io.kotest.matchers.shouldBe
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.Timeout
import java.lang.Thread.interrupted
import java.util.concurrent.CompletableFuture
import java.util.concurrent.ConcurrentLinkedQueue
import java.util.concurrent.Executors.newSingleThreadExecutor
import java.util.concurrent.Future
import java.util.concurrent.TimeUnit.SECONDS

@Timeout(value = 1L, unit = SECONDS) // Tests use threads
internal class RequestClientTest {
    private val threadPool = newSingleThreadExecutor()
    private val requestQueue = ConcurrentLinkedQueue<RemoteRequest>()
    private val client = RequestClient(requestQueue)

    @AfterEach
    fun cleanup() {
        threadPool.shutdownNow()
    }

    @Test
    fun `should succeed at one read`() {
        runFakeRequestProcessorForQuery(true, "GREEN")

        val response = client.readOne("WHAT IS YOUR FAVORITE COLOR?")

        response shouldBe "GREEN"
    }

    @Test
    fun `should fail at one read`() {
        runFakeRequestProcessorForQuery(false)

        shouldThrow<IllegalStateException> {
            client.readOne("WHAT IS YOUR FAVORITE COLOR?")
        }
    }

    @Test
    fun `should succeed at one write`() {
        runFakeRequestProcessorForQuery(true, "BLUE IS THE NEW GREEN")

        val response = client.writeOne("CHANGE COLORS")

        response shouldBe "BLUE IS THE NEW GREEN"
    }

    @Test
    fun `should fail at one write`() {
        runFakeRequestProcessorForQuery(false)

        shouldThrow<IllegalStateException> {
            client.writeOne("CHANGE COLORS")
        }
    }

    @Test
    fun `should succeed at one read in a transaction`() {
        runFakeRequestProcessorForQuery(true, "GREEN")

        val response = client.inTransaction(1).use { txn ->
            txn.readOne("WHAT IS YOUR FAVORITE COLOR?")
        }

        response shouldBe "GREEN"
    }

    @Test
    fun `should fail at one read in a transaction`() {
        runFakeRequestProcessorForQuery(false)

        client.inTransaction(1).use { txn ->
            shouldThrow<IllegalStateException> {
                txn.readOne("WHAT IS YOUR FAVORITE COLOR?")
            }
        }
    }

    @Test
    fun `should succeed at one write in a transaction`() {
        runFakeRequestProcessorForQuery(true, "BLUE IS THE NEW GREEN")

        val response = client.inTransaction(1).use { txn ->
            txn.writeOne("CHANGE COLORS")
        }

        response shouldBe "BLUE IS THE NEW GREEN"
    }

    @Test
    fun `should fail at one write in a transaction`() {
        runFakeRequestProcessorForQuery(false)

        client.inTransaction(1).use { txn ->
            shouldThrow<IllegalStateException> {
                txn.writeOne("CHANGE COLORS")
            }
        }
    }

    @Test
    fun `should cancel in a transaction`() {
        val correctRequest = runFakeRequestProcessorForOperation(false)

        client.inTransaction(1).use { txn ->
            txn.cancel()
        }

        correctRequest.get() shouldBe true
    }

    @Test
    fun `should abort in a transaction`() {
        val correctRequest = runFakeRequestProcessorForOperation(true)

        client.inTransaction(1).use { txn ->
            txn.abort("RESET COLORS")
        }

        correctRequest.get() shouldBe true
    }

    private fun runFakeRequestProcessorForQuery(
        succeedOrFail: Boolean,
        responseForSuccess: String = "",
    ) = threadPool.submit {
        while (!interrupted()) {
            val request = requestQueue.poll()
            if (null == request) continue
            val remoteQuery = request as RemoteQuery
            if (succeedOrFail) {
                remoteQuery.result.complete(
                    SuccessRemoteResult(200, responseForSuccess)
                )
            } else {
                remoteQuery.result.complete(
                    FailureRemoteResult(429, "SLOW DOWN, PARDNER")
                )
            }
            break
        }
    }

    private fun runFakeRequestProcessorForOperation(
        hasUndoInstruction: Boolean,
    ): Future<Boolean> {
        val correctRequest = CompletableFuture<Boolean>()
        threadPool.submit {
            while (!interrupted()) {
                val request = requestQueue.poll()
                if (null == request) continue
                if (request is AbandonUnitOfWork) {
                    correctRequest.complete(
                        request.undo.isNotEmpty() == hasUndoInstruction
                    )
                } else {
                    correctRequest.complete(false)
                }
                break
            }
        }
        return correctRequest
    }
}
