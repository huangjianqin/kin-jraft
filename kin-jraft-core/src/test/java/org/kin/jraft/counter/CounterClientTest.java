package org.kin.jraft.counter;

import com.alipay.sofa.jraft.entity.PeerId;
import org.kin.jraft.RaftClient;
import org.kin.jraft.RaftClientOptions;
import org.kin.jraft.counter.message.GetValueRequest;
import org.kin.jraft.counter.message.IncrementAndGetRequest;

import java.util.concurrent.CountDownLatch;

/**
 * @author huangjianqin
 * @date 2021/11/14
 */
public class CounterClientTest {
    public static void main(String[] args) throws InterruptedException {
        String clusterAddresses = args[0];
        RaftClient client = RaftClientOptions.builder()
                .groupId("counter_raft")
                .clusterAddresses(clusterAddresses)
                .connect();

        PeerId leader = client.getLeader();
        System.out.println("leader is " + leader);
        int n = 0;
        CountDownLatch latch = new CountDownLatch(n);
        long start = System.currentTimeMillis();
        for (int i = 0; i < n; i++) {
            incrementAndGet(client, 1, latch);
        }
        latch.await();
        get(client);
        Thread.sleep(1_000);
        System.out.println(n + " ops, cost : " + (System.currentTimeMillis() - start) + " ms.");
        System.exit(0);
    }

    private static void incrementAndGet(RaftClient client, long delta, CountDownLatch latch) {
        IncrementAndGetRequest request = new IncrementAndGetRequest();
        request.setDelta(delta);
        client.invokeLeaderAsync(request, (result, err) -> {
            if (err == null) {
                latch.countDown();
                System.out.println("incrementAndGet result:" + result);
            } else {
                err.printStackTrace();
                latch.countDown();
            }
        }, 5000);
    }

    private static void get(RaftClient client) {
        GetValueRequest request = new GetValueRequest();
        client.invokeLeaderAsync(request, (result, err) -> {
            if (err == null) {
                System.out.println("get result:" + result);
            } else {
                err.printStackTrace();
            }
        }, 5000);
    }
}
