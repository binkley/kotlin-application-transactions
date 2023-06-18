package hm.binkley.labs.applicationTransactions.client

interface Transactional<QueryResult, OperationResult> : AutoCloseable {
    /** Runs a single remote read query. */
    fun readOne(query: String): QueryResult

    /** Runs a single remote write query. */
    fun writeOne(query: String): QueryResult

    /**
     * Abandons the current transaction.
     * All previous remote operations have been auto-committed.
     * If undo operations are needed (ie, to undo writes) use [abort].
     *
     * @param undo Multiple parameters of query instructions
     */
    fun cancel(): OperationResult

    /**
     * Abandons the current transaction.
     * All previous remote operations have been auto-committed.
     * Use to provide "undo" instructions in support of "all-or-none" semantics.
     * If no undo operations (eg, after only reads) use [cancel].
     *
     * @param undo Multiple parameters of query instructions
     */
    fun abort(undo: List<String>): OperationResult

    /**
     * Convenience for [abort] with multiple parameters of undo instructions.
     */
    fun abort(vararg undo: String) = abort(undo.asList())
}
