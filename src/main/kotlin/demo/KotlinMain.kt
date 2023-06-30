package demo

import hm.binkley.labs.applicationTransactions.RemoteRequest
import hm.binkley.labs.applicationTransactions.SuccessRemoteResult
import hm.binkley.labs.applicationTransactions.client.RequestClient
import hm.binkley.labs.applicationTransactions.processing.RemoteResourceManager
import hm.binkley.labs.applicationTransactions.processing.RequestProcessor
import lombok.Generated
import java.util.concurrent.ConcurrentLinkedQueue
import java.util.concurrent.Executors.newCachedThreadPool

/** Runs the demo. */
@Generated // Lie to JaCoCo
fun main() {
    println("SET UP THE REQUEST PROCESSOR AND CONNECTION TO REMOTE RESOURCE")
    println("THIS EXAMPLE FOLLOWS \"HAPPY PATH\". THE REMOTE ALWAYS SUCCEEDS")
    println(
        """
        THE 3 MINIMAL DEPENDENCES ARE:
        - A SEARCHABLE REQUEST QUEUE FROM CLIENT (CALLER) AND PROCESSOR
        - A THREAD POOL FOR PARALLELLING READ REQUESTS
        - A REMOTE RESOURCE THIS PROJECT PROTECTS FROM INCONSISTENCY
        """.trimIndent()
    )
    val requestQueue = ConcurrentLinkedQueue<RemoteRequest>()
    val threadPool = newCachedThreadPool()
    val remoteRequests = mutableListOf<String>()
    val remote = RemoteResourceManager { query ->
        remoteRequests.add(query)
        SuccessRemoteResult(200, "REMOTE: $query")
    }

    val processor = RequestProcessor(requestQueue, threadPool, remote)
    threadPool.submit(processor)

    val client = RequestClient(requestQueue)

    println("RUN 10 SIMPLE READ REQUESTS IN PARALLEL")
    println("READ REQUESTS FROM CALLER BLOCK UNTIL RESULTS RETURN")
    for (i in 1..10) {
        val name = client.readOne("READ NAME")
        println("RESULT: $name")
    }
    println("WRITE SIMPLE REQUEST EXCLUSIVELY BLOCK UNTIL READS COMPLETE")
    client.writeOne("CHANGE NAME")

    println("START A UNIT OF WORK EXPECTING UP TO 2 REQUESTS")
    println(
        "UNITS OF WORK ARE EXCLUSIVE OF SIMPLE READS AND WRITES AND OF" +
            " OTHER UNITS OF WORK"
    )
    val uow = client.inExclusiveAccess(2)
    println("USE OF `use` IS KOTLIN-SPECIFIC: RELEASE RESOURCE AFTER")
    uow.use {
        println("- SEND A READ WORK UNIT, AND CAN RUN IN PARALLEL")
        val falseCondition = uow.readOne("READ CONDITION")
        if (!falseCondition.contains("READ CONDITION")) {
            uow.cancel()
            return
        }

        println("- SEND A WRITE WORK UNIT, AND RUN EXCLUSIVELY")
        val writeResult = uow.writeOne("READ COMPLEX RELATIONSHIP")
        if (!writeResult.contains("REMOTE")) {
            uow.abort("SOME UNDO INSTRUCTION", "ANOTHER UNDO INSTRUCTION")
            return
        }

        println("WRITES ARE AUTO-COMMITTED WITHOUT SPECIFIC UNDO IN ABORT")
        println("THERE MAY NOT BE ANY SENSE OF \"ISOLATION\" FOR THE REMOTE")
    }

    println("CLEANUP MAYBE HANDLED BY A FRAMEWORK")
    println(
        "REMOTE REQUESTS SENT: ${
            remoteRequests.joinToString(
                separator = "\n- ",
                prefix = "\n- ",
            )
        }"
    )
    threadPool.shutdownNow()
}
