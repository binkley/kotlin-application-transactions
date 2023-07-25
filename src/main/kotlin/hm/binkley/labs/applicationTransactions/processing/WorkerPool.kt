package hm.binkley.labs.applicationTransactions.processing

import lombok.Generated
import java.util.concurrent.Callable
import java.util.concurrent.ConcurrentLinkedQueue
import java.util.concurrent.ExecutorService
import java.util.concurrent.Future
import java.util.concurrent.TimeUnit
import java.util.concurrent.TimeoutException

/**
 * A JVM thread pool wrapper that provides:
 * - [areAllDone]
 * - [awaitCompletion]
 *
 * When ported to another language, this may be a facility provided natively
 * in thread pools.
 *
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
        for (task in runQueue)
            if (!task.isDone) return false
        return true
    }

    /**
     * Wait for all current submitted tasks (reads) to complete thus ensuring an
     * empty run queue, and that the remote resource is available for exclusive
     * access.
     *
     * Note that this sequence:
     * 1. [ExecutorService.shutdown]
     * 2. [ExecutorService.awaitTermination]
     *
     * is equivalent, but _only one time_: afterward the `ExecutorService` no
     * longer accepts new tasks.
     *
     * @return if all tasks completed within the [timeout]
     */
    fun awaitCompletion(timeout: Long, unit: TimeUnit): Boolean {
        runQueue.forEach { task ->
            try {
                task.get(timeout, unit)
            } catch (_: TimeoutException) {
                return false
            }
        }
        return true
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
