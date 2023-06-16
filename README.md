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
  * Client B writes data that would change the result of client A's read
  * Ideal is that operations are in this order: Read\[A], Write\[A], Write\[B]
  * However, interleaving client requests is possible resulting in: Read\[A],
    Write\[B], Write\[A]
- Support for _rollback_.
  A client should be able to undo changes within their transaction without 
  effecting other clients

### Goals

* Though written in Kotlin, the project may be manually translated into a
  related language having similar concepts (_eg_, Java, C#, _et al_)
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
- The target language has a thread-safe _FIFO queue_ construct supporting 
  searching through the queue (not just taking the head)

This project _does not_ address distributed transactions; it assumes a
_single_ remote data source service.

## Design

Here "caller" means those offering requests to a shared queue, and "remote" 
means a single consumer of the queue processing requests.

### Caller API

The key types are:

- [`RemoteRequest`](src/main/kotlin/hm/binkley/labs/applicationTransactions/client/RemoteRequest.kt)
  &mdash; an operation sent into the queue
- [`RemoteResponse`](src/main/kotlin/hm/binkley/labs/applicationTransactions/client/RemoteResult.kt)
  &mdash; a result from the operation, if any
- [`UnitOfWork`](src/main/kotlin/hm/binkley/labs/applicationTransactions/client/UnitOfWork.kt)
  &mdash; a container of operations with "all-or-none" semantics

Typical patterns in Kotlin would be:

```kotlin
when(result = read("ASK A QUESTION")) {
    is SuccessRemoteResult -> return someGoodThing(result.response)
    else -> throw handleFailure(result)
}
```

```kotlin
when(result = write("CHANGE SOMETHING")) {
    is SuccessRemoteResult -> return someGoodThing(result.response)
    else -> throw handleFailure(result)
}
```

```kotlin
// Multiple operations that need to be done as a group: Write depends on Read
UnitOfWork().use {
    var result = read("ASK A QUESTION")
    if (result is FailureRemoteResult) throw handleFailure(result)
    var response = result.response
    if (notTheRightAnswer(response)) return beforeWriting()

    result = write("CHANGE SOMETHING BASED ON $response")
    if (result is FailureResultResult) throw handleFailure(result)
    response = result.response
    
    return someTransformation(response)
}
```

```kotlin
// Multiple operations that need to be done as a group: Rollback on failure
UnitOfWork().use {
    var result = write("CHANGE SOMETHING")
    if (result is FailureResultResult) {
        rollback()
        throw handleFailure(result)
    }

    // Continue with rest of transaction
}
```

### Remote API
