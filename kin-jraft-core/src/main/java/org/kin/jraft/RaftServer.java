package org.kin.jraft;

import com.alipay.sofa.jraft.*;
import com.alipay.sofa.jraft.conf.Configuration;
import com.alipay.sofa.jraft.core.CliServiceImpl;
import com.alipay.sofa.jraft.entity.PeerId;
import com.alipay.sofa.jraft.option.CliOptions;
import com.alipay.sofa.jraft.option.NodeOptions;
import com.alipay.sofa.jraft.rpc.RaftRpcServerFactory;
import com.alipay.sofa.jraft.rpc.RpcServer;
import com.alipay.sofa.jraft.rpc.impl.cli.CliClientServiceImpl;
import com.google.common.collect.ImmutableMap;
import org.apache.commons.io.FileUtils;
import org.kin.framework.concurrent.SimpleThreadFactory;
import org.kin.framework.concurrent.ThreadPoolUtils;
import org.kin.framework.log.LoggerOprs;
import org.kin.framework.utils.ExceptionUtils;
import org.kin.framework.utils.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.io.File;
import java.nio.file.Paths;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * 支持multi-group raft server
 * @author huangjianqin
 * @date 2022/4/20
 */
public final class RaftServer implements Lifecycle<RaftServerOptions>, LoggerOprs {
    private static final Logger log = LoggerFactory.getLogger(RaftGroup.class);
    /** 初始状态 */
    private static final int INIT = 0;
    /** started */
    private static final int STARTED = 1;
    /** stopped */
    private static final int STOPPED = 2;

    /** raft server peer id */
    private PeerId localPeerId;
    /** raft rpc server */
    private RpcServer raftRpcServer;
    /** raft service rpc server */
    @Nullable
    private RpcServer raftServiceRpcServer;
    private RaftService raftService;
    /** 状态标识 */
    private final AtomicInteger state = new AtomicInteger(INIT);
    /** key -> group, value -> raft group */
    private Map<String, RaftGroup> raftGroupMap = Collections.emptyMap();
    /** 内置raft client, 与其他raft server通讯 */
    private CliService cliService;
    /** 内置raft client, 与其他raft server通讯 */
    private CliClientServiceImpl cliClientService;
    /** executor to handle RAFT requests */
    private ExecutorService raftRpcExecutor;
    /** executor to handle CLI service requests */
    private ExecutorService raftCliServiceRpcExecutor;
    /** 内部使用处理通用逻辑的Scheduler */
    private ScheduledExecutorService commonScheduler;
    /** executor to handle raft service requests */
    @Nullable
    private ExecutorService raftServiceRpcExecutor;
    /** executor to handle raft service CLI service requests */
    @Nullable
    private ExecutorService raftServiceCliServiceRpcExecutor;
    /** raft rpc请求timeout毫秒数 */
    private int rpcRequestTimeoutMs = 5_000;

    @Override
    public synchronized boolean init(RaftServerOptions opts) {
        if (!state.compareAndSet(INIT, STARTED)) {
            return false;
        }

        //set up common configuration
        rpcRequestTimeoutMs = opts.getRpcRequestTimeoutMs();

        //init executor
        raftRpcExecutor = ThreadPoolUtils.threadPoolBuilder()
                .poolName("raft-rpc")
                .coreThreads(opts.getRaftRpcThreadNum())
                .threadFactory(new SimpleThreadFactory("raft-rpc"))
                .common();
        raftCliServiceRpcExecutor = ThreadPoolUtils.threadPoolBuilder()
                .poolName("raft-cli-service")
                .coreThreads(opts.getRaftCliServiceRpcThreadNum())
                .threadFactory(new SimpleThreadFactory("raft-cli-service"))
                .common();
        commonScheduler = ThreadPoolUtils.scheduledThreadPoolBuilder()
                .poolName("raft-common")
                .coreThreads(4)
                .threadFactory(new SimpleThreadFactory("raft-common"))
                .build();

        //init raft node rpc server
        localPeerId = new PeerId();
        if (!localPeerId.parse(opts.getAddress())) {
            throw new IllegalArgumentException("fail to parse rpc peer id: " + opts.getAddress());
        }

        raftRpcServer = RaftRpcServerFactory.createRaftRpcServer(localPeerId.getEndpoint(), raftRpcExecutor, raftCliServiceRpcExecutor);
        if (!raftRpcServer.init(null)) {
            throw new IllegalStateException("fail to start raft rpc server");
        }

        //init raft group node
        ImmutableMap.Builder<String, RaftGroup> raftGroupMapBuilder = ImmutableMap.builder();
        for (RaftGroupOptions raftGroupOptions : opts.getRaftGroupOptionsList()) {
            initRaftGroup(opts, raftGroupOptions, raftGroupMapBuilder);
        }
        raftGroupMap = raftGroupMapBuilder.build();

        //init raft service
        //标识raft node和raft service是否使用同一个rpc server
        RaftServiceFactory raftServiceFactory = opts.getRaftServiceFactory();
        if (Objects.nonNull(raftServiceFactory)) {
            boolean isRaftNodeServiceSameRpcServer = false;
            String serviceAddress = opts.getServiceAddress();
            if (StringUtils.isBlank(serviceAddress)) {
                //默认raft node和raft service使用同一个rpc server
                isRaftNodeServiceSameRpcServer = true;
            }

            if (isRaftNodeServiceSameRpcServer) {
                raftServiceRpcServer = raftRpcServer;
            } else {
                raftServiceRpcExecutor = ThreadPoolUtils.threadPoolBuilder()
                        .poolName("raft-service-rpc")
                        .coreThreads(opts.getRaftServiceRpcThreadNum())
                        .threadFactory(new SimpleThreadFactory("raft-service-rpc"))
                        .common();
                raftServiceCliServiceRpcExecutor = ThreadPoolUtils.threadPoolBuilder()
                        .poolName("raft-service-cli-service")
                        .coreThreads(opts.getRaftServiceCliServiceRpcThreadNum())
                        .threadFactory(new SimpleThreadFactory("raft-service-cli-service"))
                        .common();

                PeerId servicePeerId = new PeerId();
                if (!servicePeerId.parse(serviceAddress)) {
                    throw new IllegalArgumentException("fail to parse service peer id: " + serviceAddress);
                }
                raftServiceRpcServer = RaftRpcServerFactory.createRaftRpcServer(servicePeerId.getEndpoint(), raftServiceRpcExecutor, raftServiceCliServiceRpcExecutor);
                if (!raftServiceRpcServer.init(null)) {
                    throw new IllegalStateException("fail to start raft service rpc server");
                }
            }

            raftService = raftServiceFactory.create(this, raftServiceRpcServer);
        }

        //init internal raft client
        CliOptions cliOptions = new CliOptions();
        cliOptions.setTimeoutMs(3000);
        cliOptions.setMaxRetry(3);
        this.cliService = com.alipay.sofa.jraft.RaftServiceFactory.createAndInitCliService(cliOptions);
        this.cliClientService = (CliClientServiceImpl) ((CliServiceImpl) this.cliService).getCliClientService();

        return true;
    }

    /**
     * 初始化raft group
     */
    private void initRaftGroup(RaftServerOptions raftServerOptions, RaftGroupOptions raftGroupOptions,
                               ImmutableMap.Builder<String, RaftGroup> raftGroupMapBuilder) {
        //node options
        NodeOptions nodeOpts = new NodeOptions();

        //init data directory
        String groupId = raftGroupOptions.getGroupId();
        initDirectory(raftServerOptions.getDataDir(), groupId, nodeOpts);

        //init statemachine
        RaftGroup raftGroup = new RaftGroup(groupId, this);
        AbstractStateMachine sm;
        StateMachineFactory stateMachineFactory = raftGroupOptions.getStateMachineFactory();
        if (Objects.nonNull(stateMachineFactory)) {
            sm = stateMachineFactory.create(this, raftGroup);
        } else {
            //election
            sm = new ElectionStateMachine(this, raftGroup);
        }
        List<NodeStateChangeListener> listeners = new ArrayList<>(8);
        listeners.addAll(raftServerOptions.getListeners());
        listeners.addAll(raftGroupOptions.getListeners());
        sm.init(raftGroupOptions.getSnapshotFileOperation(), listeners);
        nodeOpts.setFsm(sm);

        //init cluster node address
        Configuration configuration = new Configuration();
        if (!configuration.parse(raftServerOptions.getClusterAddresses())) {
            throw new IllegalArgumentException("fail to parse cluster addresses: " + raftServerOptions.getClusterAddresses());
        }
        nodeOpts.setInitialConf(configuration);

        //custom node options
        NodeOptionsCustomizer parentNodeOptionsCustomizer = raftServerOptions.getNodeOptionsCustomizer();
        if (Objects.nonNull(parentNodeOptionsCustomizer)) {
            parentNodeOptionsCustomizer.customize(nodeOpts);
        }
        NodeOptionsCustomizer nodeOptionsCustomizer = raftGroupOptions.getNodeOptionsCustomizer();
        if (Objects.nonNull(nodeOptionsCustomizer)) {
            nodeOptionsCustomizer.customize(nodeOpts);
        }

        //init raft group service
        RaftGroupService raftGroupService = new RaftGroupService(groupId, localPeerId, nodeOpts, raftRpcServer, true);
        //start raft node
        Node node = raftGroupService.start(false);

        RouteTable.getInstance().updateConfiguration(groupId, configuration);

        //update raft group
        raftGroup.setNode(node);
        raftGroup.setRaftGroupService(raftGroupService);
        raftGroup.setSm(sm);

        raftGroupMapBuilder.put(groupId, raftGroup);

        commonScheduler.execute(() -> registerToCluster(groupId, configuration));

        //turn on the leader auto refresh for this group
        Random random = new Random();
        int electionTimeoutMs = nodeOpts.getElectionTimeoutMs();
        long period = electionTimeoutMs + random.nextInt(5 * 1000);
        //比选举间隔慢几秒刷新leader
        commonScheduler.scheduleAtFixedRate(() -> refreshRouteTable(groupId),
                electionTimeoutMs, period, TimeUnit.MILLISECONDS);
    }

    /**
     * 创建数据目录
     */
    private void initDirectory(String parentDir, String groupId, NodeOptions nodeOptions) {
        String logUri = Paths.get(parentDir, groupId, "log").toString();
        String snapshotUri = Paths.get(parentDir, groupId, "snapshot").toString();
        String metaDataUri = Paths.get(parentDir, groupId, "meta-data").toString();

        try {
            FileUtils.forceMkdir(new File(logUri));
            FileUtils.forceMkdir(new File(snapshotUri));
            FileUtils.forceMkdir(new File(metaDataUri));
        } catch (Exception e) {
            ExceptionUtils.throwExt(e);
        }

        nodeOptions.setLogUri(logUri);
        nodeOptions.setRaftMetaUri(metaDataUri);
        nodeOptions.setSnapshotUri(snapshotUri);
    }

    /**
     * 将raft group添加至raft cluster
     */
    private void registerToCluster(String groupId, Configuration configuration) {
        for (; ; ) {
            try {
                List<PeerId> peerIds = cliService.getPeers(groupId, configuration);
                if (peerIds.contains(localPeerId)) {
                    return;
                }
                Status status = cliService.addPeer(groupId, configuration, localPeerId);
                if (status.isOk()) {
                    return;
                }
                warn("failed to join the cluster, retry...");
            } catch (Exception e) {
                error("failed to join the cluster, retry...", e);
            }
            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
    }

    /**
     * 自动刷新route table, 让client感知raft cluster变化
     */
    private void refreshRouteTable(String groupId) {
        if (isStopped()) {
            return;
        }

        Status status;
        try {
            RouteTable instance = RouteTable.getInstance();
            Configuration oldConf = instance.getConfiguration(groupId);
            String oldLeader = Optional.ofNullable(instance.selectLeader(groupId)).orElse(PeerId.emptyPeer())
                    .getEndpoint().toString();
            status = instance.refreshLeader(this.cliClientService, groupId, rpcRequestTimeoutMs);
            if (!status.isOk()) {
                error("fail to refresh leader for group id : {}, status is : {}", groupId, status);
            }
            status = instance.refreshConfiguration(this.cliClientService, groupId, rpcRequestTimeoutMs);
            if (!status.isOk()) {
                error("fail to refresh route configuration for group id : {}, status is : {}", groupId, status);
            }
        } catch (Exception e) {
            error("fail to refresh raft metadata info for group id : {}, error is : {}", groupId, e);
        }
    }

    @Override
    public void shutdown() {
        if (!state.compareAndSet(STARTED, STOPPED)) {
            return;
        }

        raftService.close();

        //close rpc server
        raftRpcServer.shutdown();

        //close raft service server
        if (Objects.nonNull(raftServiceRpcServer) && raftServiceRpcServer != raftRpcServer) {
            raftServiceRpcServer.shutdown();
        }

        //close raft client
        cliService.shutdown();
        cliClientService.shutdown();

        //close executor
        raftRpcExecutor.shutdown();
        raftCliServiceRpcExecutor.shutdown();
        commonScheduler.shutdown();
        if (Objects.nonNull(raftServiceRpcExecutor)) {
            raftServiceRpcExecutor.shutdown();
        }
        if (Objects.nonNull(raftServiceCliServiceRpcExecutor)) {
            raftServiceCliServiceRpcExecutor.shutdown();
        }

        log.info("[RaftNode] shutdown successfully: {}.", this);
    }

    /**
     * 检查是否started状态
     */
    private void checkStartedState(){
        if(state.get() != STARTED){
            throw new IllegalStateException("raft server is not started");
        }
    }

    /**
     * raft server和raft service是否使用同一个rpc server
     */
    public boolean isRaftServerRaftServiceSameRpcServer() {
        checkStartedState();
        return raftRpcServer == raftServiceRpcServer;
    }

    /**
     * @return 是否是started状态
     */
    public boolean isStarted(){
        return state.get() == STARTED;
    }

    /**
     * @return 是否是stopped状态
     */
    public boolean isStopped(){
        return state.get() == STOPPED;
    }

    /**
     * 根据group id获取{@link RaftGroup}实例
     * @param groupId   raft group id
     * @return  {@link RaftGroup}实例
     */
    @Nullable
    public RaftGroup getRaftGroup(String groupId) {
        checkStartedState();
        return raftGroupMap.get(groupId);
    }

    /**
     * 获取raft group leader peer id
     * @param groupId    raft group id
     * @return leader peer id
     */
    PeerId getLeader(String groupId) {
        return RouteTable.getInstance().selectLeader(groupId);
    }

    //getter
    public PeerId getLocalPeerId() {
        return localPeerId;
    }

    public ExecutorService getRaftCliServiceRpcExecutor() {
        return raftCliServiceRpcExecutor;
    }

    public RaftService getRaftService() {
        return raftService;
    }

    CliClientServiceImpl getCliClientService() {
        return cliClientService;
    }

    int getRpcRequestTimeoutMs() {
        return rpcRequestTimeoutMs;
    }
}