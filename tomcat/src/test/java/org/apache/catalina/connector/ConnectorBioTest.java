package org.apache.catalina.connector;

import java.net.HttpURLConnection;
import java.net.URL;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class ConnectorBioTest {

    private Connector connector;
    private final int PORT = 8081;
    private final int ACCEPT_COUNT = 100;

    @BeforeEach
    void setUp() {
        connector = new Connector(PORT, ACCEPT_COUNT, 30);
        connector.start();
    }

    @AfterEach
    void tearDown() {
        connector.stop();
    }

    @Test
    void testSlowControllerConcurrencyLimit() throws InterruptedException {
        int[] threadCounts = {300};

        for (int threads : threadCounts) {
            long start = System.currentTimeMillis();

            ExecutorService executor = Executors.newFixedThreadPool(threads);
            CountDownLatch latch = new CountDownLatch(threads);

            for (int i = 0; i < threads; i++) {
                executor.submit(() -> {
                    try {
                        HttpURLConnection conn = (HttpURLConnection) new URL(
                                "http://localhost:8081/slow").openConnection();
                        conn.setRequestMethod("GET");
                        conn.getResponseCode();
                    } catch (Exception e) {
                        e.printStackTrace();
                    } finally {
                        latch.countDown();
                    }
                });
            }

            latch.await();
            long totalTime = System.currentTimeMillis() - start;
            System.out.println("Threads: " + threads + ", Total time: " + totalTime + "ms");

            executor.shutdown();
        }
    }
}
