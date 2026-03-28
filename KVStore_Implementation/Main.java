public class Main {
    public static void main(String[] args) throws InterruptedException {
        KVStore kv = new KVStore();

        Thread writer = new Thread(()->{
            for(int i=0;i<1000000;i++){
                kv.put("x",i);
            }
        });
        // Thread writer2 = new Thread(()->{
        //     for(int i=0;i<100000;i++){
        //         kv.put("x",i);
        //     }
        // });
        Thread reader = new Thread(()->{
            int lastSeen = -1;
            for(int i=0;i<1000000;i++){
                Integer val = kv.get("x");
                if (val!=null && val<lastSeen){
                    System.out.println("Violation detected "+ val + " < " + lastSeen);
                }
                if (val !=null){
                    lastSeen = val;
                }
            }
        });
        // Thread reader2 = new Thread(()->{
        //     int lastSeen = -1;
        //     for(int i=0;i<10000;i++){
        //         Integer val = kv.get("x");
        //         if (val!=null && val<lastSeen){
        //             System.out.println("Violation detected "+ val + " < " + lastSeen);
        //         }
        //         if (val !=null){
        //             lastSeen = val;
        //         }
        //     }
        // });


        writer.start();
        // writer2.start();
        reader.start();
        // reader2.start();


        writer.join();
        // writer2.join();
        reader.join();
        // reader2.join();

        System.out.println("Execution finished");
    }
}
