package hm.binkley.labs.applicationTransactions.processing

import hm.binkley.labs.applicationTransactions.RemoteResult

/**
 * A test-only [RemoteResource] that:
 * 1. Wraps and calls the [realRemoteResource] to perform work
 * 2. Records for testing what queries were sent
 */
internal class TestRecordingRemoteResource(
    private val realRemoteResource: RemoteResource,
) : RemoteResource {
    val calls = mutableListOf<String>()

    override fun call(query: String): RemoteResult {
        calls += query
        return realRemoteResource.call(query)
    }
}
