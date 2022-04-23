package org.kin.jraft.springboot.counter.server;

import com.alipay.sofa.jraft.Status;
import org.kin.jraft.*;
import org.kin.jraft.springboot.EnableJRaftServer;
import org.kin.jraft.springboot.counter.processor.GetValueRequestProcessor;
import org.kin.jraft.springboot.counter.processor.IncrementAndGetRequestProcessor;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.annotation.Bean;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;

/**
 * @author huangjianqin
 * @date 2021/11/8
 */
@EnableJRaftServer
@SpringBootApplication
public class CounterRaftServiceSpringBootTest {
    public static void main(String[] args) throws InterruptedException {
        ConfigurableApplicationContext context = SpringApplication.run(CounterRaftServiceSpringBootTest.class, args);

        RaftServer raftServer = context.getBean(RaftServer.class);
        CounterRaftService counterService = (CounterRaftService) raftServer.getRaftService();
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
        context.stop();
    }

    @Bean
    public NodeStateChangeListener printListener() {
        return new NodeStateChangeListener() {

            @Override
            public void onBecomeLeader(String groupId, long term) {
                System.out.println(String.format("[%s] leader start on term: %d", groupId, term));
            }

            @Override
            public void onStepDown(String groupId, long oldTerm) {
                System.out.println(String.format("[%s] leader step down: %d", groupId, oldTerm));
            }
        };
    }

    @Bean
    public RaftServiceFactory counterRaftServiceFactory() {
        return (raftServer, rpcServer) -> {
            CounterRaftService counterService = new CounterRaftServiceImpl(raftServer);
            rpcServer.registerProcessor(new GetValueRequestProcessor(counterService));
            rpcServer.registerProcessor(new IncrementAndGetRequestProcessor(counterService));
            return counterService;
        };
    }

    @Bean
    public RaftGroupOptions counterRaftGroupOptions() {
        return RaftGroupOptions.builder(Constants.GROUP_ID)
                .nodeOptionsCustomizer(nodeOpts -> {
                    nodeOpts.setElectionTimeoutMs(1000);
                    nodeOpts.setSnapshotIntervalSecs(30);
                })
                .stateMachineFactory(CounterStateMachine::new)
                .snapshotFileOperation(new CounterSnapshotOperation())
                .build();
    }
}
