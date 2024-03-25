import org.junit.jupiter.api.Test;
import org.luyunji.tools.snowflake.Snowflake;

import java.io.IOException;
import java.net.InetAddress;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ConcurrentLinkedQueue;

public class SnowflakeTest {

    @Test
    public void testConstructor() throws IOException {
        Snowflake snowflake = new Snowflake(System.currentTimeMillis(),
                InetAddress.getByName("192.168.0.1"),
                InetAddress.getByName("192.168.0.100"),
                "test", true, 41, 10, 12,
                "/tmp/IpSnowflake.txt");
        long l = snowflake.nextId();
    }

    @Test
    public void testThread() throws IOException, InterruptedException {
        Snowflake snowflake = new Snowflake("/default.properties");
        Thread[] threads = new Thread[100];
        ConcurrentLinkedQueue<Long> queue = new ConcurrentLinkedQueue<>();
        for (int i = 0; i < 100; i++) {
            threads[i] = new Thread(() -> {
                for (int j = 0; j < 100000; j++) {
                    queue.offer(snowflake.nextId()); // 原子递增操作
                }
            });
        }
        long startTime = System.currentTimeMillis();

        for (Thread thread : threads) {
            thread.start();
        }

        for (Thread thread : threads) {
            thread.join(); // 等待所有线程执行完毕
        }

        long endTime = System.currentTimeMillis();
        Set<Long> collect = new HashSet<>(queue);
        System.out.println("Time taken: " + (endTime - startTime) + "ms");
        System.out.println("collect count: " + collect.size());
    }
}
