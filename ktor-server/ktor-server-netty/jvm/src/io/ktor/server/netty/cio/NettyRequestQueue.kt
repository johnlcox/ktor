package io.ktor.server.netty.cio

import io.ktor.server.netty.NettyApplicationCall
import io.ktor.util.internal.LockFreeLinkedListNode
import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.Job
import kotlinx.coroutines.ObsoleteCoroutinesApi
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.channels.ReceiveChannel
import org.slf4j.LoggerFactory
import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater

internal class NettyRequestQueue(internal val readLimit: Int, internal val runningLimit: Int) {
    init {
        require(readLimit > 0) { "readLimit should be positive: $readLimit" }
        require(runningLimit > 0) { "executeLimit should be positive: $runningLimit" }
    }
    private val logger = LoggerFactory.getLogger("NettyRequestQueue")

    private val incomingQueue = MyQueue(Channel<CallElement>(8))

    class MyQueue(private val delegate: Channel<CallElement>) : Channel<CallElement> by delegate {
        private val logger = LoggerFactory.getLogger("NettyRequestQueue")
        private val count = AtomicInteger(0)

        override fun offer(element: CallElement): Boolean {
            val accepted = delegate.offer(element)
            if (accepted) {count.incrementAndGet()}
            if (!accepted) logger.error ("Offer Not Accepted")

            logger.error("""Queued Call Count: ${count.get()}""")
            return accepted
        }

        override fun poll(): CallElement? {
            val polledValue = delegate.poll()

            if (polledValue != null) {
                count.decrementAndGet()
            }

//            logger.error("""Queued Call Count: ${count.get()}""")

            return polledValue;
        }

        @ObsoleteCoroutinesApi
        override suspend fun receiveOrNull(): CallElement? {
            val receivedValue = delegate.receiveOrNull()

            if (receivedValue != null) {
                count.decrementAndGet()
            }

//            logger.error("""Queued Call Count: ${count.get()}""")

            return receivedValue
        }
    }

    val elements: ReceiveChannel<CallElement> = incomingQueue

    fun schedule(call: NettyApplicationCall) {
        val element = CallElement(call)
        try {
            val scheduled = incomingQueue.offer(element)
//            LoggerFactory.getLogger("NettyRequestQueue").error("hi")
            if (!scheduled) {
                logger.error("Request Queue is full")
                throw RuntimeException("Request Queue is full")
            }
        } catch (t: Throwable) {
            logger.error("Exception: $t")
            element.tryDispose()
        }
    }

    fun close() {
        incomingQueue.close()
    }

    fun cancel() {
        incomingQueue.close()

        while (true) {
            incomingQueue.poll()?.tryDispose() ?: break
        }
    }

    @UseExperimental(ExperimentalCoroutinesApi::class)
    fun canRequestMoreEvents(): Boolean = incomingQueue.isEmpty

    internal class CallElement(val call: NettyApplicationCall) : LockFreeLinkedListNode() {
        @kotlin.jvm.Volatile
        private var scheduled: Int = 0

        private val message: Job = call.response.responseMessage

        val isCompleted: Boolean get() = message.isCompleted

        fun ensureRunning(): Boolean {
            if (Scheduled.compareAndSet(this, 0, 1)) {
                call.context.fireChannelRead(call)
                return true
            }

            return scheduled == 1
        }

        fun tryDispose() {
            if (Scheduled.compareAndSet(this, 0, 2)) {
                call.dispose()
            }
        }

        companion object {
            private val Scheduled =
                AtomicIntegerFieldUpdater.newUpdater(CallElement::class.java, CallElement::scheduled.name)!!
        }
    }
}
