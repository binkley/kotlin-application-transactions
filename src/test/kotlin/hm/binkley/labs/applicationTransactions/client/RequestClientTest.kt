package hm.binkley.labs.applicationTransactions.client

import hm.binkley.labs.applicationTransactions.CancelUnitOfWork
import hm.binkley.labs.applicationTransactions.FailureRemoteResult
import hm.binkley.labs.applicationTransactions.RemoteQuery
import hm.binkley.labs.applicationTransactions.RemoteRequest
import hm.binkley.labs.applicationTransactions.SuccessRemoteResult
import io.kotest.assertions.throwables.shouldThrow
import io.kotest.matchers.collections.shouldBeEmpty
import io.kotest.matchers.shouldBe
import io.kotest.matchers.types.shouldBeInstanceOf
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.Timeout
import java.util.concurrent.Executors.newSingleThreadExecutor
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

        shouldThrow<ExampleFrameworkException> {
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

        shouldThrow<ExampleFrameworkException> {
            client.writeOne("CHANGE COLORS")
        }
    }

    @Test
    fun `should succeed at one read in a transaction`() {
        runFakeRequestProcessorForQuery(true, "GREEN")

        val response = client.inExclusiveAccess(1).use { uow ->
            uow.readOne("WHAT IS YOUR FAVORITE COLOR?")
        }

        response shouldBe "GREEN"
    }

    @Test
    fun `should fail at one read in a transaction`() {
        runFakeRequestProcessorForQuery(false)

        client.inExclusiveAccess(1).use { uow ->
            shouldThrow<ExampleFrameworkException> {
                uow.readOne("WHAT IS YOUR FAVORITE COLOR?")
            }
        }
    }

    @Test
    fun `should succeed at one write in a transaction`() {
        runFakeRequestProcessorForQuery(true, "BLUE IS THE NEW GREEN")

        val response = client.inExclusiveAccess(1).use { uow ->
            uow.writeOne("CHANGE COLORS")
        }

        response shouldBe "BLUE IS THE NEW GREEN"
    }

    @Test
    fun `should fail at one write in a transaction`() {
        runFakeRequestProcessorForQuery(false)

        client.inExclusiveAccess(1).use { uow ->
            shouldThrow<ExampleFrameworkException> {
                uow.writeOne("CHANGE COLORS")
            }
        }
    }

    @Test
    fun `should cancel a unit of work`() {
        client.inExclusiveAccess(1).use { uow ->
            uow.cancelAndKeepChanges()

            val request = requestQueue.take()

            request.shouldBeInstanceOf<CancelUnitOfWork>()
            request.undo.shouldBeEmpty()
        }
    }

    @Test
    fun `should cancel a unit of work with undo instructions`() {
        client.inExclusiveAccess(1).use { uow ->
            uow.cancelAndUndoChanges("RESET COLORS")

            val request = requestQueue.take()

            request.shouldBeInstanceOf<CancelUnitOfWork>()
            request.undo shouldBe listOf("RESET COLORS")
        }
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
}
