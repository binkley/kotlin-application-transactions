package hm.binkley.labs.applicationTransactions.client

import hm.binkley.labs.applicationTransactions.AbandonUnitOfWork
import hm.binkley.labs.applicationTransactions.ReadWorkUnit
import hm.binkley.labs.applicationTransactions.RemoteQuery
import hm.binkley.labs.applicationTransactions.WriteWorkUnit
import java.util.UUID
import java.util.UUID.randomUUID

class UnitOfWork(val expectedUnits: Int) :
    Transactional<RemoteQuery, AbandonUnitOfWork> {
    val id: UUID = randomUUID()

    /** 1-based: pre-increment before use */
    private var currentUnit = 0

    override fun readOne(query: String): ReadWorkUnit {
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

    override fun writeOne(query: String): WriteWorkUnit {
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

    override fun cancel(): AbandonUnitOfWork {
        currentUnit = expectedUnits // Help `close` find bugs
        return AbandonUnitOfWork(id)
    }

    override fun abort(undo: List<String>): AbandonUnitOfWork {
        require(undo.isNotEmpty()) {
            "Abort with no undo instructions. Did you mean cancel?"
        }

        currentUnit = expectedUnits // Help `close` find bugs
        return AbandonUnitOfWork(id, undo)
    }

    override fun close() {
        if (expectedUnits == currentUnit) return
        error(
            "BUG: Fewer work units than expected:" +
                " expected $expectedUnits; actual: $currentUnit." +
                " Did you use cancel or abort when needed?"
        )
    }
}
