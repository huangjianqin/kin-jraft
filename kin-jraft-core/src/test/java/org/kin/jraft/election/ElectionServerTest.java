package org.kin.jraft.election;

import org.kin.jraft.NodeStateChangeListener;
import org.kin.jraft.RaftGroupOptions;
import org.kin.jraft.RaftServer;
import org.kin.jraft.RaftServerOptions;

import java.util.concurrent.TimeUnit;

/**
 * @author huangjianqin
 * @date 2021/11/8
 */
public class ElectionServerTest {
    public static void main(String[] args) throws InterruptedException {
        String address = args[0];
        String clusterAddresses = args[1];

        String[] strs = address.split(":");

        RaftServer raftServer = RaftServerOptions.builder()
                .dataDir("raft/election".concat(strs[1]))
                .address(address)
                .clusterAddresses(clusterAddresses)
                .listeners(new NodeStateChangeListener() {
                    @Override
                    public void onBecomeLeader(String groupId, long term) {
                        System.out.println(String.format("[%s] leader start on term: %d", groupId, term));
                    }

                    @Override
                    public void onStepDown(String groupId, long oldTerm) {
                        System.out.println(String.format("[%s] leader step down: %d", groupId, oldTerm));
                    }
                })
                .group(RaftGroupOptions.builder("election_raft").build())
                .build();

        Thread.sleep(TimeUnit.MINUTES.toMillis(5));

        raftServer.shutdown();
    }
}
