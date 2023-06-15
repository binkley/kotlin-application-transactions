package hm.binkley.labs.applicationTransactions.client

import java.util.*

sealed interface RemoteRequest

// TODO: No tests yet
// data class OneRead(val query: String) : RemoteRequest
// data class OneWrite(val query: String) : RemoteRequest

interface WorkUnit : RemoteRequest {
    val id: UUID
    val expectedUnits: Int

    /** 1-based */
    val currentUnit: Int
    val query: String
}

data class ReadWorkUnit(
    override val id: UUID,
    override val expectedUnits: Int,
    override val currentUnit: Int,
    override val query: String
) : WorkUnit

data class WriteWorkUnit(
    override val id: UUID,
    override val expectedUnits: Int,
    override val currentUnit: Int,
    override val query: String
) : WorkUnit

class AbandonUnitOfWork(val id: UUID) : RemoteRequest
