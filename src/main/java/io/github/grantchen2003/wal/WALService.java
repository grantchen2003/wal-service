package io.github.grantchen2003.wal;

import com.google.protobuf.Empty;
import io.grpc.Status;
import io.grpc.stub.StreamObserver;

import java.io.IOException;

public class WALService extends WALGrpc.WALImplBase{
    private final WALManager walManager = new WALManager();

    @Override
    public void create(Empty request, StreamObserver<CreateResponse> responseObserver) {
        try {
            final String walId = walManager.create();
            final CreateResponse response = CreateResponse.newBuilder().setWalId(walId).build();
            responseObserver.onNext(response);
            responseObserver.onCompleted();
        } catch (IOException e) {
            responseObserver.onError(
                    Status.INTERNAL.withDescription("Failed to create WAL").asRuntimeException()
            );
        }
    }

    @Override
    public void append(AppendRequest request, StreamObserver<Empty> responseObserver) {
        final String walId = request.getWalId();
        final String payload = request.getPayload();

        try {
            walManager.append(walId, payload);
            responseObserver.onNext(Empty.getDefaultInstance());
            responseObserver.onCompleted();
        } catch (IllegalArgumentException | IOException e) {
            responseObserver.onError(
                    Status.INTERNAL.withDescription("Failed to append to WAL").asRuntimeException()
            );
        }
    }
}
