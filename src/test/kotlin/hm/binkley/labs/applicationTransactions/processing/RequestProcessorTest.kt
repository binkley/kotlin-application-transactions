package hm.binkley.labs.applicationTransactions.processing

import hm.binkley.labs.applicationTransactions.AbandonUnitOfWork
import hm.binkley.labs.applicationTransactions.FailureRemoteResult
import hm.binkley.labs.applicationTransactions.OneRead
import hm.binkley.labs.applicationTransactions.OneWrite
import hm.binkley.labs.applicationTransactions.ReadWorkUnit
import hm.binkley.labs.applicationTransactions.RemoteRequest
import hm.binkley.labs.applicationTransactions.RemoteResult
import hm.binkley.labs.applicationTransactions.SuccessRemoteResult
import hm.binkley.labs.applicationTransactions.WriteWorkUnit
import hm.binkley.labs.applicationTransactions.client.UnitOfWork
import io.kotest.matchers.collections.shouldBeEmpty
import io.kotest.matchers.should
import io.kotest.matchers.shouldBe
import io.kotest.matchers.types.beInstanceOf
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

    @AfterEach
    fun cleanup() = threadPool.shutdown()

    @Test
    fun `should stop when the processor is interrupted`() {
        runSuccessRequestProcessor()

        threadPool.shutdownNow()

        threadPool.awaitTermination(1, SECONDS) shouldBe true
    }

    @Test
    fun `should fail syntax errors`() {
        val remoteResource = runFailRequestProcessor()

        val request = OneRead("ABCD PQRSTUV")
        requestQueue.offer(request)

        val result = request.result.get()
        result should beInstanceOf<FailureRemoteResult>()
        remoteResource.calls shouldBe listOf("ABCD PQRSTUV")
    }

    @Test
    fun `should process simple reads`() {
        val remoteResource = runSuccessRequestProcessor()

        val request = OneRead("READ NAME")
        requestQueue.offer(request)

        val result = request.result.get()
        (result as SuccessRemoteResult).response shouldBe "READ NAME: CHARLIE"
        remoteResource.calls shouldBe listOf("READ NAME")
    }

    @Test
    fun `should process simple writes`() {
        val remoteResource = runSuccessRequestProcessor()

        val request = OneWrite("WRITE NAME")
        requestQueue.offer(request)

        val result = request.result.get()
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

        read.result.get()
        write.result.get()

        remoteResource.calls shouldBe listOf("SLOW LORIS", "WRITE NAME")
    }

    @Test
    fun `should process work unit reads`() {
        val remoteResource = runSuccessRequestProcessor()

        val unitOfWork = UnitOfWork(1)
        val request = unitOfWork.readOne("READ NAME")
        requestQueue.offer(request)

        val result = request.result.get()
        (result as SuccessRemoteResult).response shouldBe "READ NAME: CHARLIE"
        remoteResource.calls shouldBe listOf("READ NAME")
        unitOfWork.completed shouldBe true
    }

    @Test
    fun `should stop unit of work when interrupted`() {
        runSuccessRequestProcessor()

        val unitOfWork = UnitOfWork(2)
        val request = unitOfWork.writeOne("WRITE NAME")
        requestQueue.offer(request)

        threadPool.shutdownNow()

        threadPool.awaitTermination(1, SECONDS) shouldBe true
    }

    @Test
    fun `should process work unit writes`() {
        val remoteResource = runSuccessRequestProcessor()

        val unitOfWork = UnitOfWork(1)
        val request = unitOfWork.writeOne("WRITE NAME")
        requestQueue.offer(request)

        val result = request.result.get()
        (result as SuccessRemoteResult).response shouldBe "WRITE NAME: CHARLIE"
        remoteResource.calls shouldBe listOf("WRITE NAME")
        unitOfWork.completed shouldBe true
    }

    @Test
    fun `should not block others if unit of work does not finish`() {
        val remoteResource = runSuccessRequestProcessor()

        val unitOfWork = UnitOfWork(2)
        val workUnit = unitOfWork.writeOne("WRITE NAME")
        requestQueue.offer(workUnit)

        val pending = OneRead("FAVORITE COLOR")
        requestQueue.offer(pending)

        // The unit of work does not get a request within a reasonable time, so
        // the simple read can progress
        SECONDS.sleep(1)

        workUnit.result.get()
        pending.result.get()

        remoteResource.calls shouldBe listOf("WRITE NAME", "FAVORITE COLOR")
        unitOfWork.completed shouldBe false
    }

    @Test
    fun `should isolate work units from simple reads`() {
        val remoteResource = runSuccessRequestProcessor()

        val unitOfWork = UnitOfWork(2)
        val writeWorkUnit = unitOfWork.writeOne("WRITE NAME")
        requestQueue.offer(writeWorkUnit)

        val interleaved = OneRead("FAVORITE COLOR")
        requestQueue.offer(interleaved)

        val readWorkUnit = unitOfWork.readOne("READ NAME")
        requestQueue.offer(readWorkUnit)

        writeWorkUnit.result.get()
        readWorkUnit.result.get()
        interleaved.result.get()

        remoteResource.calls shouldBe listOf(
            "WRITE NAME",
            "READ NAME",
            "FAVORITE COLOR",
        )
        unitOfWork.completed shouldBe true
    }

    @Test
    fun `should isolate work units from each other`() {
        val remoteResource = runSuccessRequestProcessor()

        val unitOfWorkA = UnitOfWork(2)
        val readWorkUnitA = unitOfWorkA.readOne("[A] READ NAME")
        requestQueue.offer(readWorkUnitA)

        val unitOfWorkB = UnitOfWork(2)
        val writeWorkUnitB = unitOfWorkB.readOne("[B] WRITE NAME")
        requestQueue.offer(writeWorkUnitB)

        val writeWorkUnitA = unitOfWorkA.writeOne("[A] WRITE NAME")
        requestQueue.offer(writeWorkUnitA)

        val readWorkUnitB = unitOfWorkB.writeOne("[B] READ NAME")
        requestQueue.offer(readWorkUnitB)

        readWorkUnitA.result.get()
        writeWorkUnitA.result.get()
        writeWorkUnitB.result.get()
        readWorkUnitB.result.get()

        unitOfWorkA.completed shouldBe true
        unitOfWorkB.completed shouldBe true

        remoteResource.calls shouldBe listOf(
            "[A] READ NAME",
            "[A] WRITE NAME",
            "[B] WRITE NAME",
            "[B] READ NAME",
        )
        unitOfWorkA.completed shouldBe true
        unitOfWorkB.completed shouldBe true
    }

    @Test
    fun `should cancel unit of work`() {
        val remoteResource = runSuccessRequestProcessor()

        val unitOfWork = UnitOfWork(2)
        val read = unitOfWork.readOne("FAVORITE COLOR")
        requestQueue.offer(read)
        read.result.get()
        val cancel = unitOfWork.cancel()
        requestQueue.offer(cancel)

        read.result.get()

        cancel.result.get() shouldBe true
        remoteResource.calls shouldBe listOf("FAVORITE COLOR")
        unitOfWork.completed shouldBe true
    }

    @Test
    fun `should abort unit of work with undo instructions`() {
        val remoteResource = runSuccessRequestProcessor()

        val unitOfWork = UnitOfWork(17)
        val read = unitOfWork.readOne("READ NAME")
        requestQueue.offer(read)
        val abort = unitOfWork.abort("UNDO RENAME")
        requestQueue.offer(abort)

        read.result.get()

        abort.result.get() shouldBe true
        remoteResource.calls shouldBe listOf("READ NAME", "UNDO RENAME")
        unitOfWork.completed shouldBe true
    }

    @Test
    fun `should abort unit of work with undo instructions but some fail`() {
        val remoteResource = runFailRequestProcessor()

        val unitOfWork = UnitOfWork(17)
        val read = unitOfWork.readOne("READ NAME")
        requestQueue.offer(read)
        val abort = unitOfWork.abort("ABCD PQRSTUV")
        requestQueue.offer(abort)

        read.result.get()

        abort.result.get() shouldBe false
        remoteResource.calls shouldBe listOf("READ NAME", "ABCD PQRSTUV")
        unitOfWork.completed shouldBe true
    }

    @Test
    fun `should fail with a crazy work item`() {
        val remoteResource = runSuccessRequestProcessor()

        val unitOfWork = UnitOfWork(1)
        val martian = ReadWorkUnit(
            unitOfWork.id,
            unitOfWork.expectedUnits,
            -88, // Obviously a bad work unit number
            "I AM NOT THE DROID YOU ARE LOOKING FOR"
        )
        requestQueue.offer(martian)

        martian.result.get() should beInstanceOf<FailureRemoteResult>()
        remoteResource.calls.shouldBeEmpty()
    }

    @Test
    fun `should fail abandon when out of step with previous work units`() {
        runSuccessRequestProcessor()

        val expectedUnits = 3
        val unitOfWork = UnitOfWork(expectedUnits)
        requestQueue.offer(unitOfWork.writeOne("CHANGE NAME"))

        val badAbandon = AbandonUnitOfWork(
            unitOfWork.id,
            expectedUnits - 1, // What the test really checks
        )
        requestQueue.offer(badAbandon)

        badAbandon.result.get() shouldBe false
        unitOfWork.completed shouldBe false
    }

    @Test
    fun `should fail read when out of step with previous work units`() {
        runSuccessRequestProcessor()

        val unitOfWork = UnitOfWork(3)
        requestQueue.offer(unitOfWork.writeOne("CHANGE NAME"))

        val badQuery = ReadWorkUnit(
            unitOfWork.id,
            2, // What the test really checks
            unitOfWork.expectedUnits,
            "READ NAME",
        )
        requestQueue.offer(badQuery)

        badQuery.result.get() should beInstanceOf<FailureRemoteResult>()
        unitOfWork.completed shouldBe false
    }

    @Test
    fun `should fail write when out of step with previous work units`() {
        runSuccessRequestProcessor()

        val unitOfWork = UnitOfWork(3)
        requestQueue.offer(unitOfWork.writeOne("CHANGE NAME"))

        val badQuery = WriteWorkUnit(
            unitOfWork.id,
            2, // What the test really checks
            unitOfWork.expectedUnits,
            "CHANGE NAME",
        )
        requestQueue.offer(badQuery)

        badQuery.result.get() should beInstanceOf<FailureRemoteResult>()
        unitOfWork.completed shouldBe false
    }

    @Test
    fun `should fail with out of order work units`() {
        val remoteResource = runSuccessRequestProcessor()

        val unitOfWork = UnitOfWork(2)
        val martian = ReadWorkUnit(
            unitOfWork.id,
            unitOfWork.expectedUnits,
            2, // Not the first work unit
            "I AM NOT THE DROID YOU ARE LOOKING FOR"
        )
        requestQueue.offer(martian)

        martian.result.get() should beInstanceOf<FailureRemoteResult>()
        remoteResource.calls.shouldBeEmpty()
        unitOfWork.completed shouldBe false
    }

    private fun runSuccessRequestProcessor(): TestRecordingRemoteResource =
        runRecordingRequestProcessor { query ->
            SuccessRemoteResult(200, "$query: CHARLIE")
        }

    private fun runFailRequestProcessor(): TestRecordingRemoteResource =
        runRecordingRequestProcessor { query ->
            FailureRemoteResult(400, "SYNTAX ERROR: $query")
        }

    private fun runSlowRequestProcessor(): TestRecordingRemoteResource =
        runRecordingRequestProcessor { query ->
            if (query.contains("SLOW LORIS")) {
                MILLISECONDS.sleep(600)
                SuccessRemoteResult(200, "I TOOK MY TIME ABOUT IT: $query")
            } else {
                SuccessRemoteResult(200, "$query: CHARLIE")
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
                requestQueue,
                threadPool,
                RemoteResourceManager(remoteResource)
            )
        )

        return remoteResource
    }
}
