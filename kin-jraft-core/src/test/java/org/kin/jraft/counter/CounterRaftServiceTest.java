package org.kin.jraft.counter;

import com.alipay.sofa.jraft.Status;
import org.kin.jraft.NodeStateChangeListener;
import org.kin.jraft.RaftGroupOptions;
import org.kin.jraft.RaftServer;
import org.kin.jraft.RaftServerOptions;
import org.kin.jraft.counter.processor.GetValueRequestProcessor;
import org.kin.jraft.counter.processor.IncrementAndGetRequestProcessor;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;

/**
 * @author huangjianqin
 * @date 2021/11/8
 */
public class CounterRaftServiceTest {
    private static CounterRaftService counterService;

    public static void main(String[] args) throws InterruptedException {
        String address = args[0];
        String clusterAddresses = args[1];

        String[] strs = address.split(":");

        RaftServer raftServer = RaftServerOptions.builder()
                //模拟每个节点的log目录不一致
                .dataDir("raft/counter".concat(strs[1]))
                .address(address)
                .clusterAddresses(clusterAddresses)
                .raftServiceFactory((rs, rpcServer) -> {
                    counterService = new CounterRaftServiceImpl(rs);
                    rpcServer.registerProcessor(new GetValueRequestProcessor(counterService));
                    rpcServer.registerProcessor(new IncrementAndGetRequestProcessor(counterService));
                    return counterService;
                })
                .group(RaftGroupOptions.builder(Constants.GROUP_ID)
                        .nodeOptionsCustomizer(nodeOpts -> {
                            nodeOpts.setElectionTimeoutMs(1000);
                            nodeOpts.setSnapshotIntervalSecs(30);
                        })
                        .stateMachineFactory(CounterStateMachine::new)
                        .snapshotFileOperation(new CounterSnapshotOperation())
                        .build())
                .listeners(new NodeStateChangeListener() {
                    @Override
                    public void onBecomeLeader(String groupId, long term) {
                        System.out.println(String.format("[%s] leader start on term: %d", groupId, term));
                    }

                    @Override
                    public void onStepDown(String groupId, long oldTerm) {
                        System.out.println(String.format("[%s] leader step down: %d", groupId, oldTerm));
                    }
                }).build();

        ExecutorService executorService = Executors.newFixedThreadPool(1);
        executorService.execute(() -> {
            for (int i = 0; i < 100; i++) {
                try {
                    counterService.incrementAndGet(1, new CounterClosure() {
                        @Override
                        public void run(Status status) {
                            if (status.isOk()) {
                                System.out.println(getOperation());
                            } else {
                                System.err.println(status.getErrorMsg());
                            }
                        }
                    });
                } catch (Exception e) {
                    System.err.println(e);
                }

                try {
                    Thread.sleep(ThreadLocalRandom.current().nextInt(1000));
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        });

        Thread.sleep(TimeUnit.MINUTES.toMillis(5));
        executorService.shutdown();
        raftServer.shutdown();
    }
}
