package com.aazim.kvstore.replication;

import java.util.List;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

@Component
public class AsyncReplicationStrategy implements ReplicationStrategy {
    private final List<String> followers  = List.of(
        "http://localhost:8081/kv",
        "http://localhost:8082/kv",
        "http://localhost:8085/kv"
    );

    @Override
    public boolean handleWrite(String key, String value, long timestamp){
        for (String node: nodes){

            sendAsync(node,key,value,timestamp)
        }

        return true;
    }
    
}
