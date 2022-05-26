package service;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

public class JsonConverter {
    static ObjectMapper objectMapper = new ObjectMapper();

    public static String messageToJSON(HealthMessage healthMessage){
        String json = "";
        try {
            json =  objectMapper.writeValueAsString(healthMessage);
        } catch (JsonProcessingException e) {
            e.printStackTrace();
        }
        return json;
    }

    public static HealthMessage jsonToMessage(String json){
        HealthMessage healthMessage = null;
        try {
            healthMessage = objectMapper.readValue(json, HealthMessage.class);
        } catch (JsonProcessingException e) {
            e.printStackTrace();
        }
        return healthMessage;
    }
}
