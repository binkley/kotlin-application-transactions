package hm.binkley.labs.applicationTransactions.processing

import hm.binkley.labs.applicationTransactions.FailureRemoteResult
import hm.binkley.labs.applicationTransactions.OneRead
import hm.binkley.labs.applicationTransactions.OneWrite
import hm.binkley.labs.applicationTransactions.ReadWorkUnit
import hm.binkley.labs.applicationTransactions.RemoteRequest
import hm.binkley.labs.applicationTransactions.RemoteResult
import hm.binkley.labs.applicationTransactions.SuccessRemoteResult
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
import java.util.concurrent.TimeUnit.DAYS
import java.util.concurrent.TimeUnit.MILLISECONDS
import java.util.concurrent.TimeUnit.SECONDS

/** Not typical unit test style; threads are challenging. */
@Timeout(value = 1, unit = DAYS) // Tests use threads
internal class RequestProcessorTest {
    private val requestQueue = ConcurrentLinkedQueue<RemoteRequest>()
    private val threadPool = newCachedThreadPool()

    @AfterEach
    fun cleanup() = threadPool.shutdown()

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
        val remoteResource = runPausingRequestProcessor()

        val read = OneRead("SLOW LORIS")
        requestQueue.offer(read)

        val write = OneWrite("WRITE NAME")
        requestQueue.offer(write)

        read.result.get()
        write.result.get()

        remoteResource.calls shouldBe listOf("SLOW LORIS", "WRITE NAME")
    }

    @Test
    fun `should process cancel before work begins`() {
        val remoteResource = runSuccessRequestProcessor()

        val unitOfWork = UnitOfWork(1)
        val request = unitOfWork.cancel()
        requestQueue.offer(request)

        request.result.get() shouldBe true
        remoteResource.calls.shouldBeEmpty()
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
    }

    @Test
    @Timeout(value = 2, unit = SECONDS)
    fun `should not block others if unit of work does not finish`() {
        val remoteResource = runSuccessRequestProcessor()

        val unitOfWork = UnitOfWork(2)
        val request = unitOfWork.writeOne("WRITE NAME")
        requestQueue.offer(request)

        val interleaved = OneRead("FAVORITE COLOR")
        requestQueue.offer(interleaved)

        // TODO: Arrange that test does not need bletcherous "sleep"
        SECONDS.sleep(1)

        request.result.get()
        interleaved.result.get()

        remoteResource.calls shouldBe listOf("WRITE NAME", "FAVORITE COLOR")
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

        remoteResource.calls shouldBe listOf(
            "[A] READ NAME",
            "[A] WRITE NAME",
            "[B] WRITE NAME",
            "[B] READ NAME",
        )
    }

    @Test
    @Timeout(value = 2L, unit = SECONDS)
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
    }

    @Test
    fun `should fail sending a crazy work item`() {
        val remoteResource = runSuccessRequestProcessor()

        val unitOfWork = UnitOfWork(2)
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
    fun `should fail sending out of order work units`() {
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
    }

    private fun runSuccessRequestProcessor(): TestRecordingRemoteResource =
        recordingRequestProcessor { query ->
            SuccessRemoteResult(200, "$query: CHARLIE")
        }

    private fun runFailRequestProcessor(): TestRecordingRemoteResource =
        recordingRequestProcessor { query ->
            FailureRemoteResult(400, "SYNTAX ERROR: $query")
        }

    private fun runPausingRequestProcessor(): TestRecordingRemoteResource =
        recordingRequestProcessor { query ->
            when (query) {
                "SLOW LORIS" -> {
                    MILLISECONDS.sleep(600)
                    SuccessRemoteResult(200, "I TOOK MY TIME ABOUT IT")
                }

                else -> SuccessRemoteResult(200, "$query: CHARLIE")
            }
        }

    private fun recordingRequestProcessor(
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
