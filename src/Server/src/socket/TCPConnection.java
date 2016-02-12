package socket;

import util.Utils;

import java.io.*;
import java.net.*;

public class TCPConnection {
    private final int packetLength = 1024;
    private final int maxStringLength = 1024 * 1024;

    public boolean connected = false;

    private Socket socket;
    private InputStream inputStream;
    private OutputStream outputStream;

    protected TCPConnection() {}

    public TCPConnection(Socket socket) throws IOException {
        Open(socket);
    }

    protected void Open(Socket socket) throws IOException {
        this.socket = socket;
        inputStream = socket.getInputStream();
        outputStream = socket.getOutputStream();
        connected = true;
    }

    public void Close() {
        try {
            socket.close();
            connected = false;
        } catch (Exception e) {}
    }

    public String getAddress() {
        return socket.getInetAddress().getHostAddress();
    }

    public int Receive(byte[] buffer) throws IOException {
        return inputStream.read(buffer);
    }

    public int Receive(byte[] buffer, int length) throws IOException {
        return inputStream.read(buffer, 0, length);
    }

    public String ReceiveString() throws IOException {
        byte[] buffer = new byte[packetLength];
        int len = inputStream.read(buffer);
        if (len > 0) {
            return new String(buffer, 0, len, "UTF-8");
        }
        else {
            throw new IOException("No data received.");
        }
    }

    public String ReceiveStringUntil(String endString) throws IOException {
        byte[] buffer = new byte[maxStringLength];
        int length = 0;
        do {
            int b = inputStream.read();
            if (b == -1) break;

            if (length + 1 > maxStringLength) {
                throw new IOException("Received string is too long.");
            }

            buffer[length++] = (byte)b;
        } while (!Utils.endsWith(buffer, length, endString));

        if (length == 0) throw new IOException("No data received.");

        String string = new String(buffer, 0, length, "UTF-8");
        System.out.println(string);
        return string;
    }

    public void Send(byte[] buffer, int length) throws IOException {
        outputStream.write(buffer, 0, length);
    }

    public void SendString(String message) throws IOException {
        byte[] buffer = message.getBytes("UTF-8");
        int length = buffer.length;
        int currentLength = 0;
        while (currentLength < length) {
            if (length - currentLength <= packetLength) {
                outputStream.write(buffer, currentLength, length - currentLength);
                currentLength += length - currentLength;
            }
            else {
                outputStream.write(buffer, currentLength, packetLength);
                currentLength += packetLength;
            }
        }
    }
}