package hm.binkley.labs.applicationTransactions.processing

import lombok.Generated
import java.util.concurrent.Callable
import java.util.concurrent.ConcurrentLinkedQueue
import java.util.concurrent.ExecutorService
import java.util.concurrent.Future
import java.util.concurrent.TimeUnit

/**
 * See [_How to check if all tasks running on ExecutorService are
 * completed_](https://stackoverflow.com/a/33845730).
 */
class WorkerPool(private val threadPool: ExecutorService) : AutoCloseable {
    private val runQueue = ConcurrentLinkedQueue<TrackedFuture<*>>()

    fun <T> submit(work: Callable<T>): Future<T> {
        val result = TrackedFuture(threadPool.submit(work))
        runQueue.offer(result)
        return result
    }

    fun areAllDone(): Boolean {
        var allDone = true
        for (task in runQueue) {
            allDone = allDone and task.isDone
        }
        return allDone
    }

    /**
     * Wait for all current submitted tasks to complete (reads) without
     * checking on their results, ensuring an empty run queue.
     */
    fun waitForCompletion(timeout: Long, unit: TimeUnit) =
        runQueue.forEach { task ->
            task.get(timeout, unit)
        }

    @Generated // JaCoCo+Pitest+JUnit not spotting close called in test cleanup
    override fun close() = threadPool.shutdown()

    /**
     * In a general implementation, this would also address [get] without
     * timeout.
     * Current implementation only requires `get` without timeout.
     */
    private inner class TrackedFuture<T>(private val realTask: Future<T>) :
        Future<T> by realTask {
        override fun get(timeout: Long, unit: TimeUnit): T {
            val result = realTask.get(timeout, unit) // throws TimeoutException
            runQueue.remove(this)
            return result
        }
    }
}
