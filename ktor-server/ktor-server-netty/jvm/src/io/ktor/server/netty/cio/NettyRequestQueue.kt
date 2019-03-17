package io.ktor.server.netty.cio

import io.ktor.server.netty.NettyApplicationCall
import io.ktor.util.internal.LockFreeLinkedListNode
import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.Job
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.channels.ReceiveChannel
import org.slf4j.LoggerFactory
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater

internal class NettyRequestQueue(internal val readLimit: Int, internal val runningLimit: Int) {
    init {
        require(readLimit > 0) { "readLimit should be positive: $readLimit" }
        require(runningLimit > 0) { "executeLimit should be positive: $runningLimit" }
    }

    private val incomingQueue = Channel<CallElement>(8)

    val elements: ReceiveChannel<CallElement> = incomingQueue

    fun schedule(call: NettyApplicationCall) {
        val element = CallElement(call)
        try {
            val scheduled = incomingQueue.offer(element)
//            LoggerFactory.getLogger("NettyRequestQueue").error("hi")
            if (!scheduled) {
                LoggerFactory.getLogger("NettyRequestQueue").error("Request Queue is full")
                throw RuntimeException("Request Queue is full")
            }
        } catch (t: Throwable) {
            LoggerFactory.getLogger("NettyRequestQueue").error("Exception: $t")
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
