package service;

import java.io.IOException;
import java.net.*;

public class HealthMessageSender {
    private DatagramSocket socket;
    private InetAddress address;
    private int portNo = 3500;

    private byte[] receiveBuf = new byte[2];
    private byte[] sendBuf ;

    public HealthMessageSender() {
        try {
            socket = new DatagramSocket();
        } catch (SocketException e) {
            e.printStackTrace();
        }
        try {
            address = InetAddress.getByName("localhost");
        } catch (UnknownHostException e) {
            e.printStackTrace();
        }
    }

    public void sendHealthMessage(HealthMessage healthMessage) {
        String json = JsonConverter.messageToJSON(healthMessage);
        sendBuf = json.getBytes();
        DatagramPacket packet
                = new DatagramPacket(sendBuf, sendBuf.length, address, portNo);
        try {
            socket.send(packet);
        } catch (IOException e) {
            e.printStackTrace();
        }
//        packet = new DatagramPacket(receiveBuf, receiveBuf.length);
//        try {
//            socket.receive(packet);
//        } catch (IOException e) {
//            e.printStackTrace();
//        }
//        String received = new String(
//                packet.getData(), 0, packet.getLength());
//        return received;
    }

    public void sendEndMessage(){
        sendBuf = "end".getBytes();
        DatagramPacket packet
                = new DatagramPacket(sendBuf, sendBuf.length, address, portNo);
        try {
            socket.send(packet);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void close() {
        socket.close();
    }
}