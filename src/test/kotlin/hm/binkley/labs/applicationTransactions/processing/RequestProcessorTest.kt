package hm.binkley.labs.applicationTransactions.processing

import hm.binkley.labs.applicationTransactions.CancelUnitOfWork
import hm.binkley.labs.applicationTransactions.FailureRemoteResult
import hm.binkley.labs.applicationTransactions.OneRead
import hm.binkley.labs.applicationTransactions.ReadWorkUnit
import hm.binkley.labs.applicationTransactions.RemoteQuery
import hm.binkley.labs.applicationTransactions.RemoteRequest
import hm.binkley.labs.applicationTransactions.RemoteResult
import hm.binkley.labs.applicationTransactions.SuccessRemoteResult
import hm.binkley.labs.applicationTransactions.WriteWorkUnit
import hm.binkley.labs.applicationTransactions.client.UnitOfWork
import io.kotest.matchers.collections.shouldBeEmpty
import io.kotest.matchers.collections.shouldContainOnly
import io.kotest.matchers.collections.shouldNotBeEmpty
import io.kotest.matchers.should
import io.kotest.matchers.shouldBe
import io.kotest.matchers.types.beInstanceOf
import io.kotest.matchers.types.shouldBeInstanceOf
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.Timeout
import java.util.UUID.randomUUID
import java.util.concurrent.ConcurrentLinkedQueue
import java.util.concurrent.Executors.newCachedThreadPool
import java.util.concurrent.LinkedBlockingQueue
import java.util.concurrent.TimeUnit.MILLISECONDS
import java.util.concurrent.TimeUnit.SECONDS

/**
 * Not typical unit test style; threads are challenging.
 *
 * The [Timeout] choice of 5 seconds ensures that developers do not wait
 * overlong on stuck tests, but that any pauses in production code (i.e.,
 * retries) can fully complete.
 */
@Timeout(value = 5, unit = SECONDS) // Tests use threads
internal class RequestProcessorTest {
    private val requestQueue = LinkedBlockingQueue<RemoteRequest>()
    private val threadPool = newCachedThreadPool()
    private val logger = ConcurrentLinkedQueue<String>()

    @AfterEach
    fun cleanup() = threadPool.shutdown()

    @Test
    fun `should stop when the processor is interrupted`() {
        runRequestProcessor()

        threadPool.shutdownNow()

        threadPool.awaitTermination(1, SECONDS) shouldBe true
    }

    @Test
    fun `should complain when canceling before any work is sent`() {
        val remoteResource = runRequestProcessor()

        val request = CancelUnitOfWork(randomUUID(), 1)

        requestQueue.offer(request)

        val result = ensureClientDoesNotBlock(request)
        result shouldBe true
        remoteResource.calls.shouldBeEmpty()
        shouldLogErrors()
    }

    @Test
    fun `should fail syntax errors`() {
        val remoteResource = runRequestProcessor()

        val request = OneRead("BAD: ABCD PQRSTUV")

        requestQueue.offer(request)

        val result = ensureClientDoesNotBlock(request)
        result should beInstanceOf<FailureRemoteResult>()
        remoteResource.calls shouldBe listOf("BAD: ABCD PQRSTUV")
        shouldLogErrors()
    }

    @Test
    fun `should process simple reads`() {
        val remoteResource = runRequestProcessor()

        val request = OneRead("READ NAME")

        requestQueue.offer(request)

        val result = ensureClientDoesNotBlock(request)
        (result as SuccessRemoteResult).response shouldBe "READ NAME: CHARLIE"
        remoteResource.calls shouldBe listOf("READ NAME")
    }

    @Test
    fun `should wait for reads to finish before writing`() {
        val remoteResource = runSlowRequestProcessor()

        val read = OneRead("SLOW LORIS")
        val uow = UnitOfWork(1)
        val write = uow.writeOne("WRITE NAME")

        requestQueue.offer(read)
        requestQueue.offer(write)

        ensureClientDoesNotBlock(read, write)
        remoteResource.calls shouldBe listOf("SLOW LORIS", "WRITE NAME")
    }

    @Test
    fun `should process work unit reads`() {
        val remoteResource = runRequestProcessor()

        val uow = UnitOfWork(1)
        val request = uow.readOne("READ NAME")

        requestQueue.offer(request)

        val result = ensureClientDoesNotBlock(request)
        (result as SuccessRemoteResult).response shouldBe "READ NAME: CHARLIE"
        remoteResource.calls shouldBe listOf("READ NAME")
        uow.completed shouldBe true
    }

    @Test
    fun `should process work unit writes`() {
        val remoteResource = runRequestProcessor()

        val uow = UnitOfWork(1)
        val request = uow.writeOne("WRITE NAME")

        requestQueue.offer(request)

        val result = ensureClientDoesNotBlock(request)
        (result as SuccessRemoteResult).response shouldBe "WRITE NAME: CHARLIE"
        remoteResource.calls shouldBe listOf("WRITE NAME")
        uow.completed shouldBe true
    }

    @Test
    fun `should leave unit of work when write fails`() {
        val remoteResource = runRequestProcessor()

        val uow = UnitOfWork(2)
        val workUnitWrite = uow.writeOne("BAD THING")
        val simpleRead = OneRead("READ NAME")

        requestQueue.offer(workUnitWrite)
        requestQueue.offer(simpleRead)

        ensureClientDoesNotBlock(workUnitWrite, simpleRead)
        remoteResource.calls shouldBe listOf("BAD THING", "READ NAME")
        uow.completed shouldBe false
        shouldLogErrors()
    }

    @Test
    fun `should not block others if unit of work does not finish`() {
        val remoteResource = runRequestProcessor()

        val uow = UnitOfWork(2)
        val workUnit = uow.writeOne("WRITE NAME")
        val pending = OneRead("FAVORITE COLOR")

        requestQueue.offer(workUnit)
        requestQueue.offer(pending)

        // The unit of work does not get a request within a reasonable time, so
        // the simple read can progress
        SECONDS.sleep(1)

        ensureClientDoesNotBlock(workUnit, pending)
        remoteResource.calls shouldBe listOf("WRITE NAME", "FAVORITE COLOR")
        uow.completed shouldBe false
        shouldLogErrors()
    }

    @Test
    fun `should isolate work units from simple reads`() {
        val remoteResource = runSlowReadUnitOfWorkProcessor()

        val uow = UnitOfWork(2)
        val writeWorkUnit = uow.writeOne("WRITE NAME")
        val interleaved = OneRead("FAVORITE COLOR")
        val readWorkUnit = uow.readOne("READ NAME")

        requestQueue.offer(writeWorkUnit)
        requestQueue.offer(interleaved)
        requestQueue.offer(readWorkUnit)

        // Note that the ordering of "FAVORITE COLOR" and "READ NAME" are
        // indeterminate: both are reads and run in threads.
        // "FAVORITE COLOR" is not part of the unit of work, but only needs
        // to ensure that it runs after all writes complete.

        ensureClientDoesNotBlock(writeWorkUnit, readWorkUnit, interleaved)
        uow.completed shouldBe true
        remoteResource.calls.removeFirst() shouldBe "WRITE NAME"
        remoteResource.calls shouldContainOnly
            setOf("FAVORITE COLOR", "READ NAME")
    }

    /**
     * Ordering is important:
     * 1. Start UoW "A" with a read
     * 2. Start UoW "B" with a read
     * 3. "A" proceeds with a write
     * 4. "B" waits for "A" to complete
     * Without units of work, these would interleave, and the reads would run
     * in parallel.
     */
    @Test
    fun `should isolate work units from each other`() {
        val remoteResource = runRequestProcessor()

        val uowA = UnitOfWork(2)
        val readWorkUnitA = uowA.readOne("[A] READ NAME")
        val uowB = UnitOfWork(1)
        val readWorkUnitB = uowB.readOne("[B] READ NAME")
        val writeWorkUnitA = uowA.writeOne("[A] WRITE NAME")

        requestQueue.offer(readWorkUnitA)
        requestQueue.offer(readWorkUnitB)
        requestQueue.offer(writeWorkUnitA)

        ensureClientDoesNotBlock(
            readWorkUnitA,
            writeWorkUnitA,
            readWorkUnitB,
        )
        uowA.completed shouldBe true
        uowB.completed shouldBe true
        remoteResource.calls shouldBe listOf(
            "[A] READ NAME",
            "[A] WRITE NAME",
            "[B] READ NAME",
        )
        uowA.completed shouldBe true
        uowB.completed shouldBe true
    }

    @Test
    fun `should cancel unit of work`() {
        val remoteResource = runRequestProcessor()

        val uow = UnitOfWork(2)
        val read = uow.readOne("FAVORITE COLOR")
        val cancel = uow.cancelAndKeepChanges()

        requestQueue.offer(read)
        requestQueue.offer(cancel)

        ensureClientDoesNotBlock(cancel) shouldBe true
        remoteResource.calls shouldBe listOf("FAVORITE COLOR")
        uow.completed shouldBe true
    }

    // TODO: Test sending cancel when there has been no previous requests

    @Test
    fun `should abort unit of work with undo instructions`() {
        val remoteResource = runRequestProcessor()

        val uow = UnitOfWork(17)
        val read = uow.readOne("READ NAME")
        val abort = uow.cancelAndUndoChanges("UNDO RENAME")

        requestQueue.offer(read)
        requestQueue.offer(abort)

        ensureClientDoesNotBlock(read)
        ensureClientDoesNotBlock(abort) shouldBe true

        remoteResource.calls shouldBe listOf("READ NAME", "UNDO RENAME")
        uow.completed shouldBe true
    }

    @Test
    fun `should abort unit of work with undo instructions but some fail`() {
        val remoteResource = runRequestProcessor()

        val uow = UnitOfWork(17)
        val read = uow.readOne("READ NAME")
        val abort = uow.cancelAndUndoChanges("BAD: ABCD PQRSTUV")

        requestQueue.offer(read)
        requestQueue.offer(abort)

        ensureClientDoesNotBlock(read)
        ensureClientDoesNotBlock(abort) shouldBe false // Undo failed
        remoteResource.calls shouldBe listOf("READ NAME", "BAD: ABCD PQRSTUV")
        uow.completed shouldBe true
        shouldLogErrors()
    }

    @Test
    fun `should fail with a crazy work item`() {
        val remoteResource = runRequestProcessor()

        val uow = UnitOfWork(1)
        val martian = ReadWorkUnit(
            uow.id,
            uow.expectedUnits,
            -88, // Obviously a bad work unit number
            "I AM NOT THE DROID YOU ARE LOOKING FOR"
        )

        requestQueue.offer(martian)

        val martianResult = ensureClientDoesNotBlock(martian)
        martianResult should beInstanceOf<FailureRemoteResult>()
        remoteResource.calls.shouldBeEmpty()
        shouldLogErrors()
    }

    @Test
    fun `should fail abandon when out of step with previous work units`() {
        runRequestProcessor()

        val expectedUnits = 3
        val uow = UnitOfWork(expectedUnits)
        val badAbandon = CancelUnitOfWork(
            uow.id,
            expectedUnits - 1, // What the test really checks
        )

        requestQueue.offer(uow.writeOne("CHANGE NAME"))
        requestQueue.offer(badAbandon)

        ensureClientDoesNotBlock(badAbandon) shouldBe false
        uow.completed shouldBe false
        shouldLogErrors()
    }

    @Test
    fun `should fail read when out of step with previous work units`() {
        runRequestProcessor()

        val uow = UnitOfWork(3)
        val badQuery = ReadWorkUnit(
            uow.id,
            2, // What the test really checks
            uow.expectedUnits,
            "READ NAME",
        )

        requestQueue.offer(uow.writeOne("CHANGE NAME"))
        requestQueue.offer(badQuery)

        ensureClientDoesNotBlock(badQuery) should
            beInstanceOf<FailureRemoteResult>()
        uow.completed shouldBe false
        shouldLogErrors()
    }

    @Test
    fun `should fail write when out of step with previous work units`() {
        runRequestProcessor()

        val uow = UnitOfWork(3)
        val badQuery = WriteWorkUnit(
            uow.id,
            2, // What the test really checks
            uow.expectedUnits,
            "CHANGE NAME",
        )

        requestQueue.offer(uow.writeOne("CHANGE NAME"))
        requestQueue.offer(badQuery)

        ensureClientDoesNotBlock(badQuery) should
            beInstanceOf<FailureRemoteResult>()
        uow.completed shouldBe false
        shouldLogErrors()
    }

    @Test
    fun `should fail with out of order work units`() {
        val remoteResource = runRequestProcessor()

        val uow = UnitOfWork(2)
        val martian = ReadWorkUnit(
            uow.id,
            uow.expectedUnits,
            2, // Not the first work unit
            "I AM NOT THE DROID YOU ARE LOOKING FOR"
        )

        requestQueue.offer(martian)

        ensureClientDoesNotBlock(martian) should
            beInstanceOf<FailureRemoteResult>()
        remoteResource.calls.shouldBeEmpty()
        uow.completed shouldBe false
        shouldLogErrors()
    }

    @Test
    fun `should fail for write when read requests timeout`() {
        val remoteResource = runTimingOutRequestProcessor()

        val read = OneRead("READ NAME")
        val uow = UnitOfWork(1)
        val write = uow.writeOne("WRITE NAME")

        requestQueue.offer(read)
        requestQueue.offer(write)

        val writeResult = ensureClientDoesNotBlock(write)
        writeResult.shouldBeInstanceOf<FailureRemoteResult>()
        writeResult.status shouldBe 504 // Gateway timeout
        remoteResource.calls shouldBe listOf("READ NAME")
        shouldLogErrors()
    }

    @Test
    fun `should fail for rollback when read work units timeout`() {
        val remoteResource = runTimingOutRequestProcessor()

        val unitOfWork = UnitOfWork(17)
        val read = unitOfWork.readOne("READ NAME")
        val cancel = unitOfWork.cancelAndUndoChanges("UNDO RENAME")

        requestQueue.offer(read)
        requestQueue.offer(cancel)

        ensureClientDoesNotBlock(read)
        ensureClientDoesNotBlock(cancel) shouldBe false
        remoteResource.calls shouldBe listOf("READ NAME")
        shouldLogErrors()
    }

    private fun runRequestProcessor(): TestRecordingRemoteResource =
        runRecordingRequestProcessor { query ->
            if (query.contains("BAD")) {
                FailureRemoteResult(400, query, "PROBLEM: $query")
            } else {
                SuccessRemoteResult(200, query, "$query: CHARLIE")
            }
        }

    private fun runSlowReadUnitOfWorkProcessor(): TestRecordingRemoteResource =
        runRecordingRequestProcessor { query ->
            when (query) {
                "READ NAME" -> {
                    MILLISECONDS.sleep(600)
                    SuccessRemoteResult(
                        200,
                        query,
                        "I TOOK MY TIME ABOUT IT: $query"
                    )
                }

                else -> SuccessRemoteResult(200, query, "$query: CHARLIE")
            }
        }

    private fun runSlowRequestProcessor(): TestRecordingRemoteResource =
        runRecordingRequestProcessor { query ->
            when {
                query.contains("SLOW LORIS") -> {
                    MILLISECONDS.sleep(600)
                    SuccessRemoteResult(
                        200,
                        query,
                        "I TOOK MY TIME ABOUT IT: $query",
                    )
                }

                else -> SuccessRemoteResult(200, query, "$query: CHARLIE")
            }
        }

    private fun runRecordingRequestProcessor(
        suppliedRemoteResult: (String) -> RemoteResult,
    ): TestRecordingRemoteResource {
        val remoteResource = TestRecordingRemoteResource { query ->
            suppliedRemoteResult(query)
        }

        threadPool.submit(
            RequestProcessor(
                requestQueue = requestQueue,
                remoteResource = remoteResource,
                logger = logger,
            )
        )

        return remoteResource
    }

    /**
     * The remote resource takes 2 seconds to process a read request, but the
     * processor is configured to wait only 1 second.
     */
    private fun runTimingOutRequestProcessor(): TestRecordingRemoteResource {
        val remoteResource = TestRecordingRemoteResource { query ->
            if (query.contains("READ")) {
                SECONDS.sleep(2)
            }
            SuccessRemoteResult(200, query, "$query: CHARLIE")
        }

        threadPool.submit(
            RequestProcessor(
                requestQueue = requestQueue,
                remoteResource = remoteResource,
                logger = logger,
                maxWaitForRemoteResourceInSeconds = 1,
            )
        )

        return remoteResource
    }

    private fun shouldLogErrors() {
        logger.shouldNotBeEmpty()
    }

    // If the "ensureClientDoesNotHang" methods to not get a result within
    // 5 seconds, JUnit will fail the test (look at @Timeout at the top)

    /** @return [RemoteResult] */
    private fun ensureClientDoesNotBlock(request: RemoteQuery) =
        request.result.get()

    /**
     * The [requests] need to be in same order as expected for them to complete.
     */
    private fun ensureClientDoesNotBlock(vararg requests: RemoteQuery) {
        for (request in requests)
            ensureClientDoesNotBlock(request)
    }

    /** @return [Boolean] */
    private fun ensureClientDoesNotBlock(request: CancelUnitOfWork) =
        request.result.get()
}
