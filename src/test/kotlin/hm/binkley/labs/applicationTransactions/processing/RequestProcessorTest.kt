package hm.binkley.labs.applicationTransactions.processing

import hm.binkley.labs.applicationTransactions.AbandonUnitOfWork
import hm.binkley.labs.applicationTransactions.FailureRemoteResult
import hm.binkley.labs.applicationTransactions.OneRead
import hm.binkley.labs.applicationTransactions.OneWrite
import hm.binkley.labs.applicationTransactions.ReadWorkUnit
import hm.binkley.labs.applicationTransactions.RemoteQuery
import hm.binkley.labs.applicationTransactions.RemoteRequest
import hm.binkley.labs.applicationTransactions.RemoteResult
import hm.binkley.labs.applicationTransactions.SuccessRemoteResult
import hm.binkley.labs.applicationTransactions.WriteWorkUnit
import hm.binkley.labs.applicationTransactions.client.UnitOfWork
import io.kotest.matchers.collections.shouldBeEmpty
import io.kotest.matchers.collections.shouldNotBeEmpty
import io.kotest.matchers.should
import io.kotest.matchers.shouldBe
import io.kotest.matchers.types.beInstanceOf
import io.kotest.matchers.types.shouldBeInstanceOf
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.Timeout
import java.util.concurrent.ConcurrentLinkedQueue
import java.util.concurrent.Executors.newCachedThreadPool
import java.util.concurrent.TimeUnit.MILLISECONDS
import java.util.concurrent.TimeUnit.SECONDS

/**
 * Not typical unit test style; threads are challenging.
 *
 * The [Timeout] choice of 5 seconds ensures that developers do not wait
 * overlong on stuck tests, but that any pauses in production code (ie,
 * retries) can fully complete.
 */
@Timeout(value = 5, unit = SECONDS) // Tests use threads
internal class RequestProcessorTest {
    private val requestQueue = ConcurrentLinkedQueue<RemoteRequest>()
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
    fun `should fail syntax errors`() {
        val remoteResource = runRequestProcessor()

        val request = OneRead("BAD: ABCD PQRSTUV")
        requestQueue.offer(request)

        val result = ensureClientDoesNotHang(request)

        result should beInstanceOf<FailureRemoteResult>()
        remoteResource.calls shouldBe listOf("BAD: ABCD PQRSTUV")
        shouldLogErrors()
    }

    @Test
    fun `should process simple reads`() {
        val remoteResource = runRequestProcessor()

        val request = OneRead("READ NAME")
        requestQueue.offer(request)

        val result = ensureClientDoesNotHang(request)

        (result as SuccessRemoteResult).response shouldBe "READ NAME: CHARLIE"
        remoteResource.calls shouldBe listOf("READ NAME")
    }

    @Test
    fun `should process simple writes`() {
        val remoteResource = runRequestProcessor()

        val request = OneWrite("WRITE NAME")
        requestQueue.offer(request)

        val result = ensureClientDoesNotHang(request)

        (result as SuccessRemoteResult).response shouldBe "WRITE NAME: CHARLIE"
        remoteResource.calls shouldBe listOf("WRITE NAME")
    }

    @Test
    fun `should wait for reads to finish before writing`() {
        val remoteResource = runSlowRequestProcessor()

        val read = OneRead("SLOW LORIS")
        requestQueue.offer(read)

        val write = OneWrite("WRITE NAME")
        requestQueue.offer(write)

        ensureClientDoesNotHang(read, write)

        remoteResource.calls shouldBe listOf("SLOW LORIS", "WRITE NAME")
    }

    @Test
    fun `should process work unit reads`() {
        val remoteResource = runRequestProcessor()

        val unitOfWork = UnitOfWork(1)
        val request = unitOfWork.readOne("READ NAME")
        requestQueue.offer(request)

        val result = ensureClientDoesNotHang(request)

        (result as SuccessRemoteResult).response shouldBe "READ NAME: CHARLIE"
        remoteResource.calls shouldBe listOf("READ NAME")
        unitOfWork.completed shouldBe true
    }

    @Test
    fun `should stop unit of work when interrupted`() {
        runRequestProcessor()

        val unitOfWork = UnitOfWork(2)
        val request = unitOfWork.writeOne("WRITE NAME")
        requestQueue.offer(request)

        threadPool.shutdownNow()

        threadPool.awaitTermination(1, SECONDS) shouldBe true
    }

    @Test
    fun `should process work unit writes`() {
        val remoteResource = runRequestProcessor()

        val unitOfWork = UnitOfWork(1)
        val request = unitOfWork.writeOne("WRITE NAME")
        requestQueue.offer(request)

        val result = ensureClientDoesNotHang(request)

        (result as SuccessRemoteResult).response shouldBe "WRITE NAME: CHARLIE"
        remoteResource.calls shouldBe listOf("WRITE NAME")
        unitOfWork.completed shouldBe true
    }

    @Test
    fun `should leave unit of work when write fails`() {
        val remoteResource = runRequestProcessor()

        val unitOfWork = UnitOfWork(2)
        val workUnitWrite = unitOfWork.writeOne("BAD THING")
        requestQueue.offer(workUnitWrite)

        val simpleRead = OneRead("READ NAME")
        requestQueue.offer(simpleRead)

        ensureClientDoesNotHang(workUnitWrite, simpleRead)

        remoteResource.calls shouldBe listOf("BAD THING", "READ NAME")
        unitOfWork.completed shouldBe false
        shouldLogErrors()
    }

    @Test
    fun `should not block others if unit of work does not finish`() {
        val remoteResource = runRequestProcessor()

        val unitOfWork = UnitOfWork(2)
        val workUnit = unitOfWork.writeOne("WRITE NAME")
        requestQueue.offer(workUnit)

        val pending = OneRead("FAVORITE COLOR")
        requestQueue.offer(pending)

        // The unit of work does not get a request within a reasonable time, so
        // the simple read can progress
        SECONDS.sleep(1)

        ensureClientDoesNotHang(workUnit, pending)

        remoteResource.calls shouldBe listOf("WRITE NAME", "FAVORITE COLOR")
        unitOfWork.completed shouldBe false
        shouldLogErrors()
    }

    @Test
    fun `should isolate work units from simple reads`() {
        val remoteResource = runRequestProcessor()

        val unitOfWork = UnitOfWork(2)
        val writeWorkUnit = unitOfWork.writeOne("WRITE NAME")
        requestQueue.offer(writeWorkUnit)

        val interleaved = OneRead("FAVORITE COLOR")
        requestQueue.offer(interleaved)

        val readWorkUnit = unitOfWork.readOne("READ NAME")
        requestQueue.offer(readWorkUnit)

        ensureClientDoesNotHang(writeWorkUnit, readWorkUnit, interleaved)

        remoteResource.calls shouldBe listOf(
            "WRITE NAME",
            "READ NAME",
            "FAVORITE COLOR",
        )
        unitOfWork.completed shouldBe true
    }

    /**
     * Ordering is important:
     * 1. Start UoW A with a read
     * 2. Start UoW B with a read
     * 3. A proceeds with a write
     * 4. B waits for A to complete
     * Without units of work, these would interleave, and the reads would run
     * in parallel.
     */
    @Test
    fun `should isolate work units from each other`() {
        val remoteResource = runRequestProcessor()

        val unitOfWorkA = UnitOfWork(2)
        val readWorkUnitA = unitOfWorkA.readOne("[A] READ NAME")
        requestQueue.offer(readWorkUnitA)

        val unitOfWorkB = UnitOfWork(1)
        val readWorkUnitB = unitOfWorkB.readOne("[B] READ NAME")
        requestQueue.offer(readWorkUnitB)

        val writeWorkUnitA = unitOfWorkA.writeOne("[A] WRITE NAME")
        requestQueue.offer(writeWorkUnitA)

        ensureClientDoesNotHang(
            readWorkUnitA,
            writeWorkUnitA,
            readWorkUnitB,
        )

        unitOfWorkA.completed shouldBe true
        unitOfWorkB.completed shouldBe true

        remoteResource.calls shouldBe listOf(
            "[A] READ NAME",
            "[A] WRITE NAME",
            "[B] READ NAME",
        )
        unitOfWorkA.completed shouldBe true
        unitOfWorkB.completed shouldBe true
    }

    @Test
    fun `should cancel unit of work`() {
        val remoteResource = runRequestProcessor()

        val unitOfWork = UnitOfWork(2)
        val read = unitOfWork.readOne("FAVORITE COLOR")
        requestQueue.offer(read)

        ensureClientDoesNotHang(read)

        val cancel = unitOfWork.cancel()
        requestQueue.offer(cancel)

        ensureClientDoesNotHang(cancel) shouldBe true

        remoteResource.calls shouldBe listOf("FAVORITE COLOR")
        unitOfWork.completed shouldBe true // false without cancel
    }

    @Test
    fun `should abort unit of work with undo instructions`() {
        val remoteResource = runRequestProcessor()

        val unitOfWork = UnitOfWork(17)
        val read = unitOfWork.readOne("READ NAME")
        requestQueue.offer(read)
        val abort = unitOfWork.abort("UNDO RENAME")
        requestQueue.offer(abort)

        ensureClientDoesNotHang(read)
        ensureClientDoesNotHang(abort) shouldBe true

        remoteResource.calls shouldBe listOf("READ NAME", "UNDO RENAME")
        unitOfWork.completed shouldBe true // false without cancel
    }

    @Test
    fun `should abort unit of work with undo instructions but some fail`() {
        val remoteResource = runRequestProcessor()

        val unitOfWork = UnitOfWork(17)
        val read = unitOfWork.readOne("READ NAME")
        requestQueue.offer(read)
        val abort = unitOfWork.abort("BAD: ABCD PQRSTUV")
        requestQueue.offer(abort)

        ensureClientDoesNotHang(read)
        ensureClientDoesNotHang(abort) shouldBe false // Undo failed

        remoteResource.calls shouldBe listOf("READ NAME", "BAD: ABCD PQRSTUV")
        unitOfWork.completed shouldBe true
        shouldLogErrors()
    }

    @Test
    fun `should fail with a crazy work item`() {
        val remoteResource = runRequestProcessor()

        val unitOfWork = UnitOfWork(1)
        val martian = ReadWorkUnit(
            unitOfWork.id,
            unitOfWork.expectedUnits,
            -88, // Obviously a bad work unit number
            "I AM NOT THE DROID YOU ARE LOOKING FOR"
        )
        requestQueue.offer(martian)

        val martialResult = ensureClientDoesNotHang(martian)

        martialResult should beInstanceOf<FailureRemoteResult>()
        remoteResource.calls.shouldBeEmpty()
        shouldLogErrors()
    }

    @Test
    fun `should fail abandon when out of step with previous work units`() {
        runRequestProcessor()

        val expectedUnits = 3
        val unitOfWork = UnitOfWork(expectedUnits)
        requestQueue.offer(unitOfWork.writeOne("CHANGE NAME"))

        val badAbandon = AbandonUnitOfWork(
            unitOfWork.id,
            expectedUnits - 1, // What the test really checks
        )
        requestQueue.offer(badAbandon)

        ensureClientDoesNotHang(badAbandon) shouldBe false

        unitOfWork.completed shouldBe false
        shouldLogErrors()
    }

    @Test
    fun `should fail read when out of step with previous work units`() {
        runRequestProcessor()

        val unitOfWork = UnitOfWork(3)
        requestQueue.offer(unitOfWork.writeOne("CHANGE NAME"))

        val badQuery = ReadWorkUnit(
            unitOfWork.id,
            2, // What the test really checks
            unitOfWork.expectedUnits,
            "READ NAME",
        )
        requestQueue.offer(badQuery)

        ensureClientDoesNotHang(badQuery) should
            beInstanceOf<FailureRemoteResult>()

        unitOfWork.completed shouldBe false
        shouldLogErrors()
    }

    @Test
    fun `should fail write when out of step with previous work units`() {
        runRequestProcessor()

        val unitOfWork = UnitOfWork(3)
        requestQueue.offer(unitOfWork.writeOne("CHANGE NAME"))

        val badQuery = WriteWorkUnit(
            unitOfWork.id,
            2, // What the test really checks
            unitOfWork.expectedUnits,
            "CHANGE NAME",
        )
        requestQueue.offer(badQuery)

        ensureClientDoesNotHang(badQuery) should
            beInstanceOf<FailureRemoteResult>()

        unitOfWork.completed shouldBe false
        shouldLogErrors()
    }

    @Test
    fun `should fail with out of order work units`() {
        val remoteResource = runRequestProcessor()

        val unitOfWork = UnitOfWork(2)
        val martian = ReadWorkUnit(
            unitOfWork.id,
            unitOfWork.expectedUnits,
            2, // Not the first work unit
            "I AM NOT THE DROID YOU ARE LOOKING FOR"
        )
        requestQueue.offer(martian)

        ensureClientDoesNotHang(martian) should
            beInstanceOf<FailureRemoteResult>()

        remoteResource.calls.shouldBeEmpty()
        unitOfWork.completed shouldBe false
        shouldLogErrors()
    }

    @Test
    fun `should fail for write when read requests timeout`() {
        val remoteResource = runTimingOutRequestProcessor()

        val read = OneRead("READ NAME")
        requestQueue.offer(read)

        val write = OneWrite("WRITE NAME")
        requestQueue.offer(write)

        val writeResult = ensureClientDoesNotHang(write)

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
        requestQueue.offer(read)
        val abort = unitOfWork.abort("UNDO RENAME")
        requestQueue.offer(abort)

        ensureClientDoesNotHang(read)
        ensureClientDoesNotHang(abort) shouldBe false

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

    private fun runSlowRequestProcessor(): TestRecordingRemoteResource =
        runRecordingRequestProcessor { query ->
            if (query.contains("SLOW LORIS")) {
                MILLISECONDS.sleep(600)
                SuccessRemoteResult(
                    200,
                    query,
                    "I TOOK MY TIME ABOUT IT: $query",
                )
            } else {
                SuccessRemoteResult(200, query, "$query: CHARLIE")
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
                threadPool = threadPool,
                remoteResourceManager = RemoteResourceManager(remoteResource),
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
                threadPool = threadPool,
                remoteResourceManager = RemoteResourceManager(remoteResource),
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
    private fun ensureClientDoesNotHang(request: RemoteQuery) =
        request.result.get()

    /**
     * The [requests] need to be in same order as expected for them to complete.
     */
    private fun ensureClientDoesNotHang(vararg requests: RemoteQuery) {
        for (request in requests)
            ensureClientDoesNotHang(request)
    }

    /** @return [Boolean] */
    private fun ensureClientDoesNotHang(request: AbandonUnitOfWork) =
        request.result.get()
}
