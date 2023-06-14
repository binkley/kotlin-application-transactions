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
