package hm.binkley.labs.applicationTransactions.client

import hm.binkley.labs.applicationTransactions.CancelUnitOfWork
import hm.binkley.labs.applicationTransactions.FailureRemoteResult
import hm.binkley.labs.applicationTransactions.RemoteQuery
import hm.binkley.labs.applicationTransactions.RemoteRequest
import hm.binkley.labs.applicationTransactions.SuccessRemoteResult
import io.kotest.assertions.throwables.shouldThrow
import io.kotest.matchers.shouldBe
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.Timeout
import java.util.concurrent.CompletableFuture
import java.util.concurrent.Executors.newSingleThreadExecutor
import java.util.concurrent.Future
import java.util.concurrent.LinkedBlockingQueue
import java.util.concurrent.TimeUnit.SECONDS

@Timeout(value = 1L, unit = SECONDS) // Tests use threads
internal class RequestClientTest {
    private val threadPool = newSingleThreadExecutor()
    private val requestQueue = LinkedBlockingQueue<RemoteRequest>()
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

        val response = client.inExclusiveAccess(1).use { txn ->
            txn.readOne("WHAT IS YOUR FAVORITE COLOR?")
        }

        response shouldBe "GREEN"
    }

    @Test
    fun `should fail at one read in a transaction`() {
        runFakeRequestProcessorForQuery(false)

        client.inExclusiveAccess(1).use { txn ->
            shouldThrow<IllegalStateException> {
                txn.readOne("WHAT IS YOUR FAVORITE COLOR?")
            }
        }
    }

    @Test
    fun `should succeed at one write in a transaction`() {
        runFakeRequestProcessorForQuery(true, "BLUE IS THE NEW GREEN")

        val response = client.inExclusiveAccess(1).use { txn ->
            txn.writeOne("CHANGE COLORS")
        }

        response shouldBe "BLUE IS THE NEW GREEN"
    }

    @Test
    fun `should fail at one write in a transaction`() {
        runFakeRequestProcessorForQuery(false)

        client.inExclusiveAccess(1).use { txn ->
            shouldThrow<IllegalStateException> {
                txn.writeOne("CHANGE COLORS")
            }
        }
    }

    @Test
    fun `should cancel a unit of work`() {
        val correctRequest = runFakeRequestProcessorForOperation(false)

        client.inExclusiveAccess(1).use { txn ->
            txn.cancelAndKeepChanges()
        }

        correctRequest.get() shouldBe true
    }

    @Test
    fun `should cancel a unit of work with undo instructions`() {
        val correctRequest = runFakeRequestProcessorForOperation(true)

        client.inExclusiveAccess(1).use { txn ->
            txn.cancelAndUndoChanges("RESET COLORS")
        }

        correctRequest.get() shouldBe true
    }

    private fun runFakeRequestProcessorForQuery(
        succeedOrFail: Boolean,
        responseForSuccess: String = "",
    ) = threadPool.submit {
        while (!Thread.interrupted()) {
            val request = requestQueue.take()
            val remoteQuery = request as RemoteQuery
            val result = if (succeedOrFail) {
                SuccessRemoteResult(
                    200,
                    remoteQuery.query,
                    responseForSuccess,
                )
            } else {
                FailureRemoteResult(
                    400,
                    remoteQuery.query,
                    "SYNTAX ERROR: ${remoteQuery.query}",
                )
            }
            remoteQuery.result.complete(result)
        }
    }

    private fun runFakeRequestProcessorForOperation(
        hasUndoInstruction: Boolean,
    ): Future<Boolean> {
        val correctRequest = CompletableFuture<Boolean>()
        threadPool.submit {
            while (!Thread.interrupted()) {
                val offeredRequest = requestQueue.take()
                if (offeredRequest is CancelUnitOfWork) {
                    correctRequest.complete(
                        offeredRequest.undo.isNotEmpty() == hasUndoInstruction
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
