package io.ktor.server.netty

import io.ktor.server.engine.*
import io.ktor.server.netty.cio.*
import io.ktor.server.netty.http1.*
import io.ktor.server.netty.http2.*
import io.netty.channel.*
import io.netty.channel.socket.SocketChannel
import io.netty.handler.codec.http.*
import io.netty.handler.codec.http2.*
import io.netty.handler.ssl.*
import io.netty.handler.timeout.*
import io.netty.util.concurrent.*
import org.slf4j.LoggerFactory
import java.nio.channels.*
import java.security.*
import java.security.cert.*
import java.util.concurrent.atomic.AtomicInteger
import kotlin.coroutines.*

/**
 * A [ChannelInitializer] implementation that does setup the default ktor channel pipeline
 */
@EngineAPI
class NettyChannelInitializer(
    private val enginePipeline: EnginePipeline,
    private val environment: ApplicationEngineEnvironment,
    private val callEventGroup: EventExecutorGroup,
    private val engineContext: CoroutineContext,
    private val userContext: CoroutineContext,
    private val connector: EngineConnectorConfig,
    private val requestQueueLimit: Int,
    private val runningLimit: Int,
    private val responseWriteTimeout: Int,
    private val httpServerCodec: () -> HttpServerCodec
) : ChannelInitializer<SocketChannel>() {
    private var sslContext: SslContext? = null
    private val allCount = AtomicInteger(0)

    init {
        if (connector is EngineSSLConnectorConfig) {

            // It is better but netty-openssl doesn't support it
//              val kmf = KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm())
//              kmf.init(ktorConnector.keyStore, password)
//              password.fill('\u0000')

            @Suppress("UNCHECKED_CAST")
            val chain1 = connector.keyStore.getCertificateChain(connector.keyAlias).toList() as List<X509Certificate>
            val certs = chain1.toList().toTypedArray()
            val password = connector.privateKeyPassword()
            val pk = connector.keyStore.getKey(connector.keyAlias, password) as PrivateKey
            password.fill('\u0000')

            sslContext = SslContextBuilder.forServer(pk, *certs).apply {
                if (alpnProvider != null) {
                    sslProvider(alpnProvider)
                    ciphers(Http2SecurityUtil.CIPHERS, SupportedCipherSuiteFilter.INSTANCE)
                    applicationProtocolConfig(
                        ApplicationProtocolConfig(
                            ApplicationProtocolConfig.Protocol.ALPN,
                            ApplicationProtocolConfig.SelectorFailureBehavior.NO_ADVERTISE,
                            ApplicationProtocolConfig.SelectedListenerFailureBehavior.ACCEPT,
                            ApplicationProtocolNames.HTTP_2,
                            ApplicationProtocolNames.HTTP_1_1
                        )
                    )
                }
            }
                .build()
        }
    }

    @Suppress("KDocMissingDocumentation")
    override fun initChannel(ch: SocketChannel) {
        with(ch.pipeline()) {
            if (connector is EngineSSLConnectorConfig) {
                addLast("ssl", sslContext!!.newHandler(ch.alloc()))

                if (alpnProvider != null) {
                    addLast(NegotiatedPipelineInitializer())
                } else {
                    configurePipeline(this, ApplicationProtocolNames.HTTP_1_1)
                }
            } else {
                configurePipeline(this, ApplicationProtocolNames.HTTP_1_1)
            }
        }
    }

    private fun configurePipeline(pipeline: ChannelPipeline, protocol: String) {
        when (protocol) {
            ApplicationProtocolNames.HTTP_2 -> {
                val handler = NettyHttp2Handler(enginePipeline, environment.application, callEventGroup, userContext, allCount)
                pipeline.addLast(Http2MultiplexCodecBuilder.forServer(handler).build())
            }
            ApplicationProtocolNames.HTTP_1_1 -> {
                LoggerFactory.getLogger("NettyChannelInitializer").error("configuring pipeline")
                val requestQueue = NettyRequestQueue(requestQueueLimit, runningLimit)
                val handler = NettyHttp1Handler(
                    enginePipeline,
                    environment,
                    callEventGroup,
                    engineContext,
                    userContext,
                    requestQueue,
                    allCount
                )

                with(pipeline) {
                    //                    addLast(LoggingHandler(LogLevel.WARN))
                    addLast("codec", httpServerCodec())
                    addLast("continue", HttpServerExpectContinueHandler())
                    addLast("timeout", WriteTimeoutHandler(responseWriteTimeout))
                    addLast("idleTimeout", IdleStateHandler(10, 10, 10))
                    addLast("myIdleHandler", object : ChannelDuplexHandler() {
                        override fun userEventTriggered(ctx: ChannelHandlerContext?, evt: Any?) {
                            if (evt is IdleStateEvent) {
                                if (evt == IdleStateEvent.ALL_IDLE_STATE_EVENT) {
                                    ctx?.close()
                                }
                            }
                        }
                    })
//                             {@code @Override}
//                             public void userEventTriggered({@link ChannelHandlerContext} ctx, {@link Object} evt) throws {@link Exception} {
//                                 if (evt instanceof {@link IdleStateEvent}) {
//                                     {@link IdleStateEvent} e = ({@link IdleStateEvent}) evt;
//                                     if (e.state() == {@link IdleState}.READER_IDLE) {
//                                             ctx.close();
//                                         } else if (e.state() == {@link IdleState}.WRITER_IDLE) {
//                                             ctx.writeAndFlush(new PingMessage());
//                                         }
//                                 }
//                             }
//                         })
                    addLast("http1", handler)

                }

                pipeline.context("codec").fireChannelActive()
            }
            else -> {
                environment.log.error("Unsupported protocol $protocol")
                pipeline.close()
            }
        }
    }

    private inner class NegotiatedPipelineInitializer :
        ApplicationProtocolNegotiationHandler(ApplicationProtocolNames.HTTP_1_1) {
        override fun configurePipeline(ctx: ChannelHandlerContext, protocol: String) =
            configurePipeline(ctx.pipeline(), protocol)

        override fun handshakeFailure(ctx: ChannelHandlerContext, cause: Throwable?) {
            if (cause is ClosedChannelException) {
                // connection closed during TLS handshake: there is no need to log it
                ctx.close()
            } else {
                super.handshakeFailure(ctx, cause)
            }
        }
    }

    @EngineAPI
    companion object {
        internal val alpnProvider by lazy { findAlpnProvider() }

        private fun findAlpnProvider(): SslProvider? {
            try {
                Class.forName("sun.security.ssl.ALPNExtension", true, null)
                return SslProvider.JDK
            } catch (ignore: Throwable) {
            }

            try {
                if (OpenSsl.isAlpnSupported()) {
                    return SslProvider.OPENSSL
                }
            } catch (ignore: Throwable) {
            }

            return null
        }
    }
}
