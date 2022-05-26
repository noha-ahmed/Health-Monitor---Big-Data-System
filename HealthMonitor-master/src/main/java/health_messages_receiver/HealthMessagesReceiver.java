package health_messages_receiver;

import java.io.IOException;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.SocketException;

//import hdfs_writer.HDFSWriter;
import writer.HDFSWriter;
import message.HealthMessage;

public class HealthMessagesReceiver extends Thread {

    private DatagramSocket socket;
    private boolean running;
    private int endCounter = 2;
    private byte[] receiveBuf = new byte[1024];
//    private byte[] sendBuf = new byte[2];
    private HealthMessage[] healthMessages = new HealthMessage[1024];
    private int messagesCounter = 0;
private HDFSWriter hdfsWriter;
    public static double[] arrivalTime = new double[1024];
    //private healthMessage[] msg = new healthMessage[1024];

    public HealthMessagesReceiver() throws SocketException {
        socket = new DatagramSocket(3500);
        hdfsWriter = new HDFSWriter();
    }

    public void run() {
        running = true;

        while (running) {
            DatagramPacket packet = new DatagramPacket(receiveBuf, receiveBuf.length);
            try {
                // receive message
                socket.receive(packet);
            } catch (IOException e) {
                e.printStackTrace();
            }

            // get json from message
            String receivedJson = new String(packet.getData(), 0, packet.getLength());
//            System.out.println(receivedJson);
            if (receivedJson.equals("end")) {
                endCounter--;
                // if server got 4 ends one from each microservice it ends
                if(endCounter == 0){
                    running = false;
                    System.out.println(messagesCounter);
                }
                continue;
            }
            HealthMessage message = JsonConverter.jsonToMessage(receivedJson);
            arrivalTime[messagesCounter] = System.currentTimeMillis();
            healthMessages[messagesCounter] = message;
            messagesCounter ++;
            if(messagesCounter == 1024){
                // send messages to hdfs
                hdfsWriter.writeMessageToHDFS(healthMessages);
                //healthMessages = new HealthMessage[1024];
                healthMessages = new HealthMessage[1024];
                messagesCounter = 0;
            }
//            InetAddress address = packet.getAddress();
//            int port = packet.getPort();
//            sendBuf = "OK".getBytes();
//            packet = new DatagramPacket(sendBuf, sendBuf.length, address, port);
//            //System.out.println(message);
//            // sending OK ack
//            try {
//                socket.send(packet);
//            } catch (IOException e) {
//                e.printStackTrace();
//            }
        }
        socket.close();
    }
}