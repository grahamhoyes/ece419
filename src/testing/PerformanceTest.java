package testing;

import app_kvServer.KVServer;

import java.util.Random;

public class PerformanceTest {

    private static final int numRequests = 10000;
    private KVServer server;

    long totalTimePut = 0;
    long totalTimeGet = 0;
    int numGetRequests = 0;
    int numPutUpdateRequests = 0;
    int numPutInsertRequests = 0;

    Random r = new Random();

    public PerformanceTest() {
        try {
            server = new KVServer(50000, "127.0.0.1:50000", "127.0.0.1", 2181);
            server.clearStorage();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }


    public void sendPutRequest(String key) {
        long start = System.nanoTime();
        boolean updated = false;
        try {
            updated = server.putKV(key, "value");
        } catch (Exception ignored) {
        }
        long end = System.nanoTime();
        totalTimePut += (end - start) / 1000;
        if (updated)
            numPutUpdateRequests++;
        else
            numPutInsertRequests++;
    }

    public void sendGetRequest(String key) {
        long start = System.nanoTime();
        try {
            server.getKV(key);
        } catch (Exception ignored) {
        }
        long end = System.nanoTime();
        totalTimeGet += (end - start) / 1000;
        numGetRequests++;
    }

    public void runTest() {
        System.out.println("Total number of requests sent per iteration: " + numRequests);
        System.out.println("The unit for time is microseconds");
        System.out.println();
        for (double ratio = 0.1; ratio<=0.9; ratio+=0.1) {

            for (int i = 0; i<numRequests; i++) {
                char c1 = (char) (r.nextInt(26) + 'a');
                char c2 = (char) (r.nextInt(26) + 'a');
                char c3 = (char) (r.nextInt(4) + 'a');
                String key = "" + c1 + c2 + c3;
                if (Math.random() < ratio)
                    sendPutRequest(key);
                else
                    sendGetRequest(key);
            }

            int numPutRequests = numPutUpdateRequests + numPutInsertRequests;

            System.out.println("Number of put insert requests: " + numPutInsertRequests);
            System.out.println("Number of put update requests: " + numPutUpdateRequests);
            System.out.println("Number of put requests total: " + numPutRequests);
            System.out.println("Number of get requests: " + numGetRequests);
            System.out.println("Ratio of put requests to total: " + ((float) numPutRequests/numRequests));
            System.out.println("Total time taken: " + (totalTimeGet + totalTimePut));
            System.out.println("Total time for put requests: " + totalTimePut);
            System.out.println("Total time for get requests: " + totalTimeGet);
            System.out.println("Average time for a put request: " + (totalTimePut/numPutRequests));
            System.out.println("Average time for a get request: " + (totalTimeGet/numGetRequests));
            System.out.println();
            try {
                server.clearStorage();
            } catch (Exception e) {
                e.printStackTrace();
            }
            totalTimePut = 0;
            totalTimeGet = 0;
            numGetRequests = 0;
            numPutInsertRequests = 0;
            numPutUpdateRequests = 0;
        }
    }

    public static void main(String[] args)  {
        new PerformanceTest().runTest();
    }

}
