package hm.binkley.labs.util

import java.util.AbstractQueue
import java.util.concurrent.BlockingQueue
import java.util.concurrent.TimeUnit
import java.util.concurrent.TimeUnit.DAYS

/**
 * A blocking queue that may be searched for non-head elements matching a
 * filter.
 *
 * When using a filter, this blocking queue tracks non-matching elements in
 * their encounter order as they are removed from the [shared] underlying
 * blocking queue, later providing non-matching elements in encounter order
 * if they were originally in the queue after matching elements.
 * This preserves FIFO ordering after matching elements are removed.
 *
 * Internally, a "spillover" list maintains elements that did not match the
 * filter while presenting a blocking queue interface so that matching
 * elements may be removed while retaining FIFO semantics.
 *
 * This splits the queue into two halves:
 * 1. The "front" half holding elements that did not match a previous filter
 *    while maintaining their FIFO ordering
 * 2. The "back" half of blocking queue elements for elements not yet
 *    encountered drawing from the shared queue
 */
class SearchableBlockingQueue<T : Any>(
    /** The blocking queue shared with producers. */
    private val shared: BlockingQueue<T>
) : AbstractQueue<T>(), BlockingQueue<T> {
    private val spillover: MutableList<T> = mutableListOf()

    /**
     * This iterator does not poll (on timeout) or block, but checks the
     * underlying [spillover] list and [shared] blocking queue.
     */
    override fun iterator(): MutableIterator<T> {
        return object : MutableIterator<T> {
            private val spilloverItr = spillover.iterator()
            private val sharedItr = shared.iterator()
            private var lookingAtSpillover = true

            override fun hasNext(): Boolean =
                if (lookingAtSpillover) {
                    val hasNext = spilloverItr.hasNext()
                    if (hasNext) {
                        true
                    } else {
                        lookingAtSpillover = false
                        hasNext()
                    }
                } else {
                    sharedItr.hasNext()
                }

            override fun next(): T =
                if (lookingAtSpillover) {
                    spilloverItr.next()
                } else {
                    sharedItr.next()
                }

            override fun remove() {
                if (lookingAtSpillover) {
                    spilloverItr.remove()
                } else {
                    // TODO: This is broken for C# and other languages that do
                    //  not support removing from blocking iterators
                    sharedItr.remove()
                }
            }
        }
    }

    override fun peek(): T =
        if (spillover.isNotEmpty()) {
            spillover.first()
        } else {
            shared.peek()
        }

    override fun poll(): T? =
        if (spillover.isNotEmpty()) {
            spillover.removeFirst()
        } else {
            shared.poll()
        }

    override fun poll(timeout: Long, unit: TimeUnit): T? =
        if (spillover.isNotEmpty()) {
            spillover.removeFirst()
        } else {
            shared.poll(timeout, unit)
        }

    /**
     * Polls the queue for a next element which matches [filter].
     * This searches through the queue finding the first matching element
     * equivalent to a timeout of 0.
     *
     * @see BlockingQueue.poll
     */
    fun poll(filter: (T) -> Boolean): T? = poll(0L, DAYS, filter)

    /**
     * Polls the queue for a next element which matches [filter] given a
     * timeout.
     * This searches through the queue finding the first matching element.
     *
     * @see BlockingQueue.poll
     */
    fun poll(timeout: Long, unit: TimeUnit, filter: (T) -> Boolean): T? {
        val spilloverItr = spillover.iterator()
        while (spilloverItr.hasNext()) {
            val next = spilloverItr.next()
            if (filter(next)) {
                spilloverItr.remove()
                return next
            }
        }

        var next = shared.poll(timeout, unit)
        while (null != next) {
            if (filter(next)) return next
            spillover.add(next)
            next = shared.poll(timeout, unit)
        }

        return null
    }

    override fun take(): T =
        if (spillover.isNotEmpty()) {
            spillover.removeFirst()
        } else {
            shared.take()
        }

    /**
     * Remaining capacity is defined by the shared [BlockingQueue].
     * Any spillover after removing from the interior of the queue is limited
     * only by memory.
     */
    override fun remainingCapacity(): Int = shared.remainingCapacity()

    override fun drainTo(c: MutableCollection<in T>): Int {
        var drained = 0
        drained += spillover.size
        c.addAll(spillover)
        spillover.clear()
        drained += shared.drainTo(c)
        return drained
    }

    override fun drainTo(c: MutableCollection<in T>, maxElements: Int): Int {
        var drained = 0
        while (drained < maxElements && spillover.isNotEmpty()) {
            c.add(spillover.removeFirst())
            ++drained
        }
        drained += shared.drainTo(c, maxElements - drained)
        return drained
    }

    override fun put(e: T) = shared.put(e)

    override val size: Int
        // TODO: Overflow?
        get() = spillover.size + shared.size

    override fun offer(e: T, timeout: Long, unit: TimeUnit) =
        shared.offer(e, timeout, unit)

    override fun offer(e: T) = shared.offer(e)
}
