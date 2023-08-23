package hm.binkley.labs.util

import java.util.AbstractQueue
import java.util.concurrent.BlockingQueue
import java.util.concurrent.TimeUnit

/**
 * This is a blocking queue that may be searched for earlier elements
 * matching a criteria.
 * It uses a private "spillover" list to maintain elements that failed to
 * match a criteria while remaining as a FIFO queue for those elements not
 * matching the criteria and any following elements.
 */
class SearchableBlockingQueue<T>(
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

    fun poll(filter: (T) -> Boolean): T? {
        val spilloverItr = spillover.iterator()
        while (spilloverItr.hasNext()) {
            val next = spilloverItr.next()
            if (filter(next)) {
                spilloverItr.remove()
                return next
            }
        }

        var next = shared.poll()
        while (null != next) {
            if (filter(next)) return next
            spillover.add(next)
            next = shared.poll()
        }

        return null
    }

    // TODO: Implement
    /*
    fun poll(timeout: Long, unit: TimeUnit, filter: (T) -> Boolean): T? {
        TODO("Not yet implemented")
    }
     */

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
        var drainTo = 0
        drainTo += spillover.size
        c.addAll(spillover)
        spillover.clear()
        drainTo += shared.drainTo(c)
        return drainTo
    }

    override fun drainTo(c: MutableCollection<in T>, maxElements: Int): Int {
        var n = 0
        while (n < maxElements && spillover.isNotEmpty()) {
            c.add(spillover.removeFirst())
            ++n
        }
        n += shared.drainTo(c, maxElements - n)
        return n
    }

    override fun put(e: T) = shared.put(e)

    override val size: Int
        // TODO: Overflow?
        get() = spillover.size + shared.size

    override fun offer(e: T, timeout: Long, unit: TimeUnit) =
        shared.offer(e, timeout, unit)

    override fun offer(e: T) = shared.offer(e)
}
