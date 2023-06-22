<a href="LICENSE.md">
<img src="https://unlicense.org/pd-icon.png" alt="Public Domain" align="right"/>
</a>

# Kotlin Application Transactions

[![build](https://github.com/binkley/kotlin-application-transactions/workflows/build/badge.svg)](https://github.com/binkley/kotlin-application-transactions/actions)
[![issues](https://img.shields.io/github/issues/binkley/kotlin-application-transactions.svg)](https://github.com/binkley/kotlin-application-transactions/issues/)
[![pull requests](https://img.shields.io/github/issues-pr/binkley/kotlin-application-transactions.svg)](https://github.com/binkley/kotlin-application-transactions/pulls)
[![vulnerabilities](https://snyk.io/test/github/binkley/kotlin-application-transactions/badge.svg)](https://snyk.io/test/github/binkley/kotlin-application-transactions)
[![license](https://img.shields.io/badge/license-Public%20Domain-blue.svg)](http://unlicense.org/)

Experiment with application-side transactions.

This is also a good project to copy as a Kotlin starter following [modern JVM
build practices](https://github.com/binkley/modern-java-practices).

## Build and try

To build, use `./mvnw clean verify`.
Try `./run` for a demonstration.

To build as CI would, use `./batect build`.
Try `./batect run` for a demonstration as CI would.

This project assumes JDK 17.
There are no run-time dependencies beyond the Kotlin standard library.

## Overview

### Motivation

Not all remote data sources provide transactions, yet clients wish to have
exclusive access for limited periods of time to ensure consistency across
multiple related operations.

Important problems to handle when multiple clients update a remote data source:

- Ensuring proper ordering of data changes, and avoiding interleaved updates
  that change the final state of data.
  An example:
    * Client A reads data, runs logic against that, and sends an update based
      on the logic
    * After A's read but before its write, client B writes data that would 
      change the result of client A's read
    * Ideal is that operations are in this order: Read\[A], Write\[A], Write\[B]
    * However, interleaving client requests is possible resulting in: Read\[A],
      Write\[B], Write\[A]
- Support for _rollback_.
  A client should be able to undo changes within their transaction without
  effecting other clients

### Goals

* Though written in Kotlin, the project may be manually translated into
  language/platforms having similar concepts (_eg_, Java, C#, Python, _et al_)
* Reads may run in parallel.
  If no write happens, then all reads are idempotent
* Writes happen in serial.
  Writes do not step on each other, and no reads happen while writing
* Units of work never interleave or overlap

### Key terms

- Read &mdash; an idempotent operation that does not modify any remote state
- Write &mdash; any operation that modifies remote state
- Unit of work &mdash; collections of remote operations that have
  "all-or-none" semantics, and do not interleave with other operations or
  units of work

### Assumptions and limitations

This project demonstrates one approach to application-side transactions.
To keep the example within a single local process, there are some
abstractions that need translation into an actual distributed scenario:

- Clients &mdash; treated as separate local threads: in a true distributed
  scenario these would be multiple processes
- Remote resource &mdash; treated as an independent local thread: in a true
  distributed scenario this would be a remote service
- Exception hierarchy &mdash; for languages/platforms with exceptions, clients
  handle application-specific exceptions rather a generic "it failed" exception
- This project _does not_ address distributed transactions; it assumes a
  _single_ remote data source service

The minimal abstractions are:

1. A searchable, concurrent-safe queue for submitting requests for remote 
   processing
2. A way to submit concurrent work to a remote resource that can discover 
   when there is no current work in progress

## Design

Here "caller" means those offering requests to a shared queue, and "processor"
means a single consumer of the queue processing requests.

### Caller API

At a top level use
[RequestClient](src/main/kotlin/hm/binkley/labs/applicationTransactions/client/RequestClient.kt).
An example session might be:

```kotlin
val client = RequestClient(requestQueue) // Queue is shared with processor

val data = client.readOne("A REMOTE READ")
if ("OK" == data)
    client.writeOne("CHANGE SOME DATA") // Changes are "auto committed"

client.inTransaction(2 /* expected max calls */).use { txn ->
    val status = txn.readOne("CHECK SOMETHING")
    if ("OK" != status) {
        txn.cancel() // Example of early return from transaction
        return
    }
    
    val result = txn.writeOne("CHANGE SOMETHING")
    if (result is FailRemoteResult) {
        txn.abort("SOME UNDO INSTRUCTION") // Example of manual rollback
    }
    
    // Transaction is automatically "closed"
}
```

### Processor API

Start processing requests by running the processor on an independent thread:

```kotlin
threadPool.submit(
    RequestProcessor(requestQueue, threadPool, remoteResource)
)
```

where specific to your language `requestQueue` is a thread-safe FIFO queue 
that is searchable, `threadPool` is a facility for starting and managing 
threads, and `remoteResource` represents calling the remote resource you 
would like to provide transactions for. 
