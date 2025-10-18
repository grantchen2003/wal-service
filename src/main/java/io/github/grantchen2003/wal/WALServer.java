package io.github.grantchen2003.wal;

import io.grpc.Server;
import io.grpc.ServerBuilder;

public class WALServer {
    public static void main(String[] args) throws Exception {
        final int port = 8080;
        final Server server = ServerBuilder.forPort(port)
                .addService(new WALService())
                .build()
                .start();

        System.out.println("WALService server started on port " + port);
        server.awaitTermination();
    }
}
