package service;

import java.io.*;
import java.time.Instant;
import java.util.Arrays;
import java.util.Comparator;


public class Main {
    public static void main(String[] args) {
        File dataFolder = new File("health_data");
        File [] files = dataFolder.listFiles();
        Arrays.sort(files, new Comparator<File>() {
            @Override
            public int compare(File o1, File o2) {
                int n1 = extractNumber(o1.getName());
                int n2 = extractNumber(o2.getName());
                return n1 - n2;
            }

            private int extractNumber(String name) {
                int i = 0;
                try {
                    int s = name.indexOf('_')+1;
                    int e = name.lastIndexOf('.');
                    String number = name.substring(s, e);
                    i = Integer.parseInt(number);
                } catch(Exception e) {
                    i = 0; // if filename does not match the format
                    // then default to 0
                }
                return i;
            }
        });
        HealthMessageSender client = new HealthMessageSender();
        for(File file : files){
            System.out.println(file.getPath());
            try {
                FileReader fileReader = new FileReader(file);
                BufferedReader br = new BufferedReader(fileReader);
                String line =  br.readLine();
                line = line.replace('\'','\"');
                line = line.replace("CPU","cpu");
                line = line.replace("RAM","ram");
                line = line.replace("Disk","disk");
                line = line.replace("Timestamp","timestamp");
                line = line.replace("Total","total");
                line = line.replace("Free","free");
                String[] jsons = line.split("}}");
                for(String json : jsons){
                    HealthMessage message = JsonConverter.jsonToMessage(json + "}}");
                    client.sendHealthMessage(message);
                }
            } catch (FileNotFoundException e) {
                e.printStackTrace();
            } catch (IOException e) {
                e.printStackTrace();
            }


            try {
                Thread.sleep(7000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
        client.sendEndMessage();
        client.close();
    }
}