package hm.binkley.labs.applicationTransactions.client

/**
 * Represents a transaction-like interface to a remote resource.
 *
 * Good use handles instances of [Transactionish] in a "using" or
 * "try-with-resources" context.
 */
interface Transactionish<QueryResult, OperationResult> : AutoCloseable {
    /** Runs a single remote read query. */
    fun readOne(query: String): QueryResult

    /** Runs a single remote write query. */
    fun writeOne(query: String): QueryResult

    /**
     * Abandons the current exclusive access.
     * All previous remote operations have been auto-committed.
     * If undo operations are needed (ie, to undo writes) use
     * [cancelAndUndoChanges].
     */
    fun cancelAndKeepChanges(): OperationResult

    /**
     * Abandons the current exclusive access.
     * All previous remote operations have been auto-committed.
     * Use to provide "undo" instructions in support of "all-or-none" semantics.
     * If no undo operations (eg, after only reads) use [cancelAndKeepChanges].
     *
     * @param undo Multiple parameters of query instructions
     */
    fun cancelAndUndoChanges(undo: List<String>): OperationResult

    /**
     * Convenience for [cancelAndUndoChanges] with multiple parameters of undo instructions
     * rather than just a list.
     */
    fun cancelAndUndoChanges(vararg undo: String) =
        cancelAndUndoChanges(undo.asList())
}
