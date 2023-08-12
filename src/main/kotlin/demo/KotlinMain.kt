package demo

import hm.binkley.labs.applicationTransactions.FailureRemoteResult
import hm.binkley.labs.applicationTransactions.RemoteRequest
import hm.binkley.labs.applicationTransactions.SuccessRemoteResult
import hm.binkley.labs.applicationTransactions.client.RequestClient
import hm.binkley.labs.applicationTransactions.processing.RemoteResourceWithBusyRetry
import hm.binkley.labs.applicationTransactions.processing.RequestProcessor
import lombok.Generated
import java.util.concurrent.BlockingQueue
import java.util.concurrent.ExecutorService
import java.util.concurrent.Executors.newCachedThreadPool
import java.util.concurrent.LinkedBlockingQueue

/**
 * Runs the demo.
 * Threading is tough:
 * Note fetching remote values even with failure to ensure timing works right.
 * This is dependent on your language and framework.
 */
@Generated // Lie to JaCoCo
fun main() {
    println("SET UP THE REQUEST PROCESSOR AND CONNECTION TO REMOTE RESOURCE")
    println("THIS EXAMPLE FOLLOWS \"HAPPY PATH\". THE REMOTE ALWAYS SUCCEEDS")
    println(
        """
        THE 3 MINIMAL DEPENDENCIES ARE:
        - A SEARCHABLE REQUEST QUEUE FROM CLIENT (CALLER) AND PROCESSOR
        - A THREAD POOL FOR PARALLELLING READ REQUESTS
        - A REMOTE RESOURCE THIS PROJECT PROTECTS FROM INCONSISTENCY
        """.trimIndent()
    )
    val requestQueue = LinkedBlockingQueue<RemoteRequest>()
    val threadPool = newCachedThreadPool()
    val remoteRequests = mutableListOf<String>()
    val remoteResource = demoRemoteResourceManager(remoteRequests)

    val logger = LinkedBlockingQueue<String>()
    logToConsole(threadPool, logger)

    val processor =
        RequestProcessor(
            requestQueue,
            threadPool,
            remoteResource,
            logger,
        )
    threadPool.submit(processor)

    val client = RequestClient(requestQueue)

    println("ALL ABOVE CODE IN THE DEMO WAS SETUP")

    println("RUN 10 SIMPLE READ REQUESTS IN PARALLEL")
    println("READ REQUESTS FROM CALLER BLOCK UNTIL RESULTS RETURN")

    for (i in 1..10) {
        val name = client.readOne("READ NAME")
        println("RESULT: $name")
    }

    println(
        "WRITE SINGLE REQUEST EXCLUSIVELY BLOCK UNTIL READS COMPLETE" +
            " WITHIN A UNIT OF WORK." +
            " UNITS OF WORK ARE EXCLUSIVE OF SIMPLE READS AND WRITES AND OF" +
            " OTHER UNITS OF WORK"
    )

    client.inExclusiveAccess(1).use { uow ->
        println(uow.writeOne("CHANGE NAME"))
    }

    println("START A UNIT OF WORK EXPECTING UP TO 2 REQUESTS")
    val uow = client.inExclusiveAccess(2)
    println("USE OF `use` IS KOTLIN-SPECIFIC: RELEASE RESOURCE AFTER")
    uow.use {
        println("- SEND A READ WORK UNIT, AND CAN RUN IN PARALLEL")
        var name = uow.readOne("READ NAME")
        println(name)
        if (!name.contains("READ NAME")) {
            uow.cancelAndKeepChanges()
            return
        }

        println("- SEND A WRITE WORK UNIT, AND RUN EXCLUSIVELY")
        name = uow.writeOne("SOMETHING FANCY WITH NAME")
        println(name)
        if (!name.contains("REMOTE")) {
            uow.cancelAndUndoChanges(
                "SOME UNDO INSTRUCTION",
                "ANOTHER UNDO INSTRUCTION"
            )
            return
        }
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

fun demoRemoteResourceManager(remoteRequests: MutableList<String>) =
    RemoteResourceWithBusyRetry({ query ->
        remoteRequests.add(query)

        when {
            query.contains("NAME") ->
                SuccessRemoteResult(200, query, "REMOTE: $query")

            else ->
                FailureRemoteResult(400, query, "BAD SYNTAX: $query")
        }
    })

/**
 * Reading through the code, this is "noise":
 * It prints log messages in the background.
 * A better strategy would use a logging facility.
 */
private fun logToConsole(
    threadPool: ExecutorService,
    logger: BlockingQueue<String>,
) {
    threadPool.submit {
        while (!Thread.interrupted()) {
            println(logger.take())
        }
    }
}
