package com.icloud

import com.google.common.annotations.VisibleForTesting
import com.icloud.extensions.logger
import io.grpc.Server
import io.grpc.ServerBuilder

@VisibleForTesting
class AutoCloseableServer
private constructor(val server: Server) : AutoCloseable {
    companion object {
        private val LOG = AutoCloseableServer::class.logger()

        @JvmStatic
        fun of(server: Server): AutoCloseableServer =
            AutoCloseableServer(server)

        @JvmStatic
        fun of(port: Int): AutoCloseableServer =
            of(runRpc(port))

        private fun runRpc(port: Int): Server =
            ServerBuilder.forPort(port).addService(RpcService).build()
    }

    override fun close() {
        this.server.shutdown()
            .apply { LOG.info("server shutdown...") }
    }


}