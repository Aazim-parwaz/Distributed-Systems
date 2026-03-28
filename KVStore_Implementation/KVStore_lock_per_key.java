import java.util.concurrent.ConcurrentHashMap;
import java.util.Map;

public class KVStore_lock_per_key {
    private Map<String,Integer> store = new ConcurrentHashMap<>();
    private Map<String,Object> locks = new ConcurrentHashMap<>();

    private Object getLock(String key){
        locks.putIfAbsent(key, new Object());
        return locks.get(key);
    }

    public void put(String key, int value){
        Object lock = getLock(key);

        synchronized (lock){
            store.put(key, value);
        }
    }

    public Integer get(String key){
        Object lock = getLock(key);

        synchronized (lock){
            return store.get(key);
        }
    }
}
