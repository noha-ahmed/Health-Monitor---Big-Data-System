package health_messages_receiver;

import writer.HDFSWriter;

import java.net.SocketException;

public class Main {
    public static void main(String[] args){
        try {
            HDFSWriter writer = new HDFSWriter();
            HealthMessagesReceiver healthMessagesReceiver = new HealthMessagesReceiver();
            healthMessagesReceiver.start();
        } catch (SocketException e) {
            e.printStackTrace();
        }

    }
}