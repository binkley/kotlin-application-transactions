package hm.binkley.labs.applicationTransactions.client

import java.util.UUID
import java.util.UUID.randomUUID

class UnitOfWork(val expectedUnits: Int) : AutoCloseable {
    val id: UUID = randomUUID()

    /** 1-based: pre-increment before use */
    private var currentUnit = 0

    fun readOne(query: String): ReadWorkUnit {
        val thisUnit = ++currentUnit
        if (expectedUnits < thisUnit) {
            error(
                "BUG: More work units than expected:" +
                    " expected $expectedUnits; actual: $thisUnit"
            )
        }

        return ReadWorkUnit(
            id,
            expectedUnits,
            thisUnit,
            query,
        )
    }

    fun writeOne(query: String): WriteWorkUnit {
        val thisUnit = ++currentUnit
        if (expectedUnits < thisUnit) {
            error(
                "BUG: More work units than expected:" +
                    " expected $expectedUnits; actual: $thisUnit"
            )
        }

        return WriteWorkUnit(
            id,
            expectedUnits,
            thisUnit,
            query,
        )
    }

    /**
     * Abandons the current unit-of-work with the remote service.
     * Although all remote operations are auto-committed, this is useful when
     * leaving a unit-of-work early on a success path without needing
     * [abort].
     * No need to call `commit` in the normal path of submitting the expected
     * number of requests.
     */
    fun cancel(): AbandonUnitOfWork {
        currentUnit = expectedUnits // Help `close` find bugs
        return AbandonUnitOfWork(id)
    }

    /**
     * Abandons the current unit-of-work with the remote service.
     * All remote operations are auto-committed.
     * Use to provide "undo" instructions in support of "all-or-none" semantics.
     * Note that if abandoning after only performing reads, no [undo]
     * instructions are needed.
     *
     * @param undo A list of query instructions
     *
     * @see cancel
     */
    fun abort(undo: List<String>): AbandonUnitOfWork {
        require(undo.isNotEmpty()) {
            "Abort with no undo instructions. Did you mean cancel?"
        }

        currentUnit = expectedUnits // Help `close` find bugs
        return AbandonUnitOfWork(id, undo)
    }

    /**
     * Abandons the current unit-of-work with the remote service.
     * All remote operations are auto-committed.
     * Use to provide "undo" instructions in support of "all-or-none" semantics.
     * Note that if abandoning after only performing reads, no [undo]
     * instructions are needed.
     *
     * @param undo Multiple parameters of query instructions
     *
     * @see cancel
     */
    fun abort(vararg undo: String): AbandonUnitOfWork = abort(undo.asList())

    override fun close() {
        if (expectedUnits == currentUnit) return
        error(
            "BUG: Fewer work units than expected:" +
                " expected $expectedUnits; actual: $currentUnit." +
                " Did you use cancel or abort when needed?"
        )
    }
}
