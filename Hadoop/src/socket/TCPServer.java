package socket;

import java.io.*;
import java.net.*;
import java.util.*;

public class TCPServer {
    public boolean running;

    private ServerSocket serverSocket;
    private List<TCPConnection> connectionList;

    public TCPServer(int port) throws IOException {
        serverSocket = new ServerSocket(port);
        connectionList = new ArrayList<>();
        running = true;
    }

    public void Stop() {
        try {
            connectionList.forEach(TCPConnection::Close);
            connectionList.clear();
            serverSocket.close();
            running = false;
        }
        catch (Exception e) {}
    }

    public TCPConnection Accept() throws IOException {
        TCPConnection connection = new TCPConnection(serverSocket.accept());
        connectionList.add(connection);
        return connection;
    }
}