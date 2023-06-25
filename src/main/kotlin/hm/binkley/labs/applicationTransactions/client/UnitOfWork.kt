package hm.binkley.labs.applicationTransactions.client

import hm.binkley.labs.applicationTransactions.AbandonUnitOfWork
import hm.binkley.labs.applicationTransactions.ReadWorkUnit
import hm.binkley.labs.applicationTransactions.RemoteQuery
import hm.binkley.labs.applicationTransactions.UnitOfWorkScope
import hm.binkley.labs.applicationTransactions.WriteWorkUnit
import java.util.UUID
import java.util.UUID.randomUUID

class UnitOfWork(val expectedUnits: Int) :
    Transactional<RemoteQuery, AbandonUnitOfWork> {
    val id: UUID = randomUUID()

    /** 1-based: pre-increment before use */
    private var currentUnit = 0

    /**
     * Checks that this unit of work was completed by running all of the
     * [expectedUnits] work units, or that it was abandoned part way through
     * ([cancel]/[abort]).
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

    override fun cancel(): AbandonUnitOfWork {
        currentUnit = expectedUnits // Help `close` find bugs
        return AbandonUnitOfWork(id, expectedUnits, currentUnit)
    }

    override fun abort(undo: List<String>): AbandonUnitOfWork {
        require(undo.isNotEmpty()) {
            "Abort with no undo instructions. Did you mean cancel?" +
                " (id: $id)"
        }

        currentUnit = expectedUnits // Help `close` find bugs
        return AbandonUnitOfWork(id, expectedUnits, currentUnit, undo)
    }

    override fun close() {
        if (expectedUnits == currentUnit) return
        error(
            "BUG: Fewer work units than expected:" +
                " expected $expectedUnits; actual: $currentUnit." +
                " Did you use cancel or abort when needed?" +
                " (id: $id)"
        )
    }
}
