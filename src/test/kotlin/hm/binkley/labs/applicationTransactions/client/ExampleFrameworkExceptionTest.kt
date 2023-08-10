package hm.binkley.labs.applicationTransactions.client

import io.kotest.matchers.shouldBe
import org.junit.jupiter.api.Test

internal class ExampleFrameworkExceptionTest {
    @Test
    fun `should have properties`() {
        val e = ExampleFrameworkException(
            3,
            "What is the capitol of Assyria?",
            "I don't know that!",
        )

        e.status shouldBe 3
        e.query shouldBe "What is the capitol of Assyria?"
        e.errorMessage shouldBe "I don't know that!"
    }

    @Test
    fun `should have a smart exception message`() {
        val e = ExampleFrameworkException(
            status = 3,
            query = "What is the capitol of Assyria?",
            errorMessage = "I don't know that!",
        )

        e.status shouldBe 3
        e.query shouldBe "What is the capitol of Assyria?"
        e.errorMessage shouldBe "I don't know that!"
        e.message shouldBe
            "I don't know that!;" +
            " remote status code: 3;" +
            " remote query: What is the capitol of Assyria?"
    }

    @Test
    fun `should have a smart exception message with incomplete information`() {
        val e1 = ExampleFrameworkException(status = 3)

        e1.message shouldBe "Unsure what happened; remote status code: 3"

        val e2 = ExampleFrameworkException(
            status = 3,
            query = "What is your favorite color?",
        )

        e2.message shouldBe "Unsure what happened;" +
            " remote status code: 3;" +
            " remote query: What is your favorite color?"

        val e3 = ExampleFrameworkException(
            status = 3,
            errorMessage = "Blue, no teal",
        )

        e3.message shouldBe "Blue, no teal; remote status code: 3"
    }
}
