package hm.binkley.labs.applicationTransactions.client

import java.util.*
import java.util.UUID.randomUUID

class UnitOfWork(val expectedUnits: Int) : AutoCloseable {
    val id: UUID = randomUUID()

    /** 1-based: pre-increment before use */
    private var currentUnit = 0

    fun read(query: String): ReadWorkUnit {
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

    fun write(query: String): WriteWorkUnit {
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

    fun rollback() = AbandonUnitOfWork(id)

    override fun close() {
        TODO("SEND Abandon if not all units sent")
    }
}
