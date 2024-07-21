package com.icloud

import com.icloud.proto.RpcServiceGrpc
import com.icloud.proto.Service.*
import io.grpc.stub.StreamObserver

object RpcService : RpcServiceGrpc.RpcServiceImplBase() {
    override fun resolve(
        request: Request,
        responseObserver: StreamObserver<Response>,
    ): Unit =
        responseObserver.onNext(getResponseFor(request))
            .also { responseObserver.onCompleted() }


    override fun resolveBatch(
        requestBatch: RequestList,
        responseObserver: StreamObserver<ResponseList>,
    ) {
        val builder = ResponseList.newBuilder()
        for (r in requestBatch.requestList) {
            builder.addResponse(getResponseFor(r))
        }
        responseObserver.onNext(builder.build())
        responseObserver.onCompleted()
    }

    private fun getResponseFor(request: Request): Response =
        Response.newBuilder().setOutput(request.input.length).build()
}