import java.util.HashMap;
import java.util.Map;

public class KVStore {
    private Map<String, Integer> store;
    
    public KVStore(){
        this.store = new HashMap<>();
    }

    public synchronized void put(String key, int value){
        store.put(key, value);
    }
    public synchronized Integer get(String key){
        return store.get(key);
    }

}
