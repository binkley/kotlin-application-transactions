package hm.binkley.labs.applicationTransactions.client

import hm.binkley.labs.applicationTransactions.CancelUnitOfWork
import hm.binkley.labs.applicationTransactions.ReadWorkUnit
import hm.binkley.labs.applicationTransactions.RemoteQuery
import hm.binkley.labs.applicationTransactions.UnitOfWorkScope
import hm.binkley.labs.applicationTransactions.WriteWorkUnit
import java.util.UUID
import java.util.UUID.randomUUID

/**
 * A unit of work is conceptually like a transaction.
 * Processing is exclusive of simple reads/writes, and of other units of work.
 *
 * The [UnitOfWork] is _stateful_.
 * Internally, it tracks the current work unit # to provide consistency checking
 * and better error messages.
 */
class UnitOfWork(
    /**
     * Support autoclose-like semantics for a unit of work after all expected
     * requests are encountered.
     * This is also helpful for detecting usage bugs where caller should use
     * [cancelAndKeepChanges] or [cancelAndUndoChanges] to end a unit of work early.
     *
     * @todo Is this the best approach? Better might be to _require_ that
     *       callers must use a "using" or "try-with-resources" idiom to
     *       guarantee the unit of work is closed ("committed")
     */
    val expectedUnits: Int
) : Transactionish<RemoteQuery, CancelUnitOfWork> {
    val id: UUID = randomUUID()

    /** 1-based: pre-increment before use */
    private var currentUnit = 0

    /**
     * Checks that this unit of work was completed by running all of the
     * [expectedUnits] work units, or that it was abandoned part way through
     * ([cancelAndKeepChanges]/[cancelAndUndoChanges]).
     *
     * Note: there may be outstanding work in progress running against the
     * remote resource.
     * This only checks that the state has seen all work units, or is abandoned.
     *
     * @see UnitOfWorkScope.isLastWorkUnit
     */
    val completed get() = expectedUnits == currentUnit

    override fun readOne(query: String): ReadWorkUnit {
        val thisUnit = ++currentUnit
        if (expectedUnits < thisUnit) {
            error(
                "BUG: More work units than expected:" +
                    " expected $expectedUnits; actual: $thisUnit" +
                    " (id: $id)"
            )
        }

        return ReadWorkUnit(id, expectedUnits, thisUnit, query)
    }

    override fun writeOne(query: String): WriteWorkUnit {
        val thisUnit = ++currentUnit
        if (expectedUnits < thisUnit) {
            error(
                "BUG: More work units than expected:" +
                    " expected $expectedUnits; actual: $thisUnit" +
                    " (id: $id)"
            )
        }

        return WriteWorkUnit(id, expectedUnits, thisUnit, query)
    }

    override fun cancelAndKeepChanges(): CancelUnitOfWork {
        currentUnit = expectedUnits // Help `close` find bugs
        return CancelUnitOfWork(id, expectedUnits)
    }

    override fun cancelAndUndoChanges(undo: List<String>): CancelUnitOfWork {
        require(undo.isNotEmpty()) {
            "No undo instructions. Did you mean cancelAndKeepChanges?" +
                " (id: $id)"
        }

        currentUnit = expectedUnits // Help `close` find bugs
        return CancelUnitOfWork(id, expectedUnits, undo)
    }

    override fun close() {
        if (expectedUnits == currentUnit) return
        error(
            "BUG: Fewer work units than expected:" +
                " expected $expectedUnits; actual: $currentUnit." +
                " Did you use cancelAndKeepChanges or cancelAndUndoChanges" +
                " as needed?" +
                " (id: $id)"
        )
    }
}
