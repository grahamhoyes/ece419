package testing;

import app_kvServer.KVServer;
import logger.LogSetup;
import org.apache.log4j.Level;

import java.io.IOException;
import java.util.Random;

public class PerformanceTest {

    private static final int numRequests = 1000;
    private KVServer server;

    Random r = new Random();

    public PerformanceTest() {
        try {
            server = new KVServer(50000, 10, "FIFO");
            server.clearStorage();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public void sendPutRequests(int n) {
        for (int i=0; i<n; i++) {
            char c1 = (char) (r.nextInt(26) + 'a');
            char c2 = (char) (r.nextInt(26) + 'a');
            char c3 = (char) (r.nextInt(26) + 'a');
            try {
                String key = "" + c1 + c2 + c3;
                server.putKV(key, "value");
            } catch (Exception e) {
            }
        }
    }


    public void sendGetRequests(int n) {
        for (int i=0; i<n; i++) {
            char c1 = (char)(r.nextInt(26) + 'a');
            char c2 = (char)(r.nextInt(26) + 'a');
            char c3 = (char)(r.nextInt(26) + 'a');
            try {
                String key = "" + c1 + c2 + c3;
                server.getKV(key);
            }
            catch (Exception e) {
            }
        }
    }


    public void runTest() {
        System.out.println("Total number of requests sent per iteration: " + numRequests);
        System.out.println("The unit for time is microseconds");
        System.out.println();
        for (double ratio = 0.1; ratio<=0.9; ratio+=0.1) {
            long startPut = System.nanoTime();
            int numPutRequests = (int) (ratio * numRequests);
            sendPutRequests(numPutRequests);
            long endPut = System.nanoTime();
            long totalTimePut = (endPut - startPut) / 1000;

            long startGet = System.nanoTime();
            int numGetRequests = (int) ((1 - ratio) * numRequests);
            sendGetRequests(numGetRequests);
            long endGet = System.nanoTime();
            long totalTimeGet = (endGet - startGet) / 1000;

            System.out.println("Ratio of put requests to total: " + ratio);
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
        }
    }

    public static void main(String[] args)  {
        new PerformanceTest().runTest();
    }

}
