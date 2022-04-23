package org.kin.jraft;

import com.google.common.base.Preconditions;
import org.kin.framework.utils.StringUtils;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

/**
 * multi-group raft server配置
 *
 * @author huangjianqin
 * @date 2022/4/20
 */
public final class RaftServerOptions {
    /** 数据存储目录 */
    private String dataDir = "raft";
    /** local node绑定的ip:port */
    private String address;
    /** raft node集群的ip:port,ip:port,ip:port */
    private String clusterAddresses;
    /** ip:port, raft service绑定地址 */
    private String serviceAddress;
    /** {@link RaftService}实现类构建逻辑 */
    private RaftServiceFactory raftServiceFactory;
    /** {@link NodeStateChangeListener} list */
    private final List<NodeStateChangeListener> listeners = new ArrayList<>();
    /** {@link RaftGroupOptions} list */
    private final List<RaftGroupOptions> raftGroupOptionsList = new ArrayList<>();
    /** 自定义raft group配置 */
    private NodeOptionsCustomizer nodeOptionsCustomizer;
    /** executor to handle RAFT requests */
    private int raftRpcThreadNum = 8;
    /** executor to handle CLI service requests */
    private int raftCliServiceRpcThreadNum = 4;
    /** executor to handle raft service requests */
    private int raftServiceRpcThreadNum = 4;
    /** executor to handle raft service CLI service requests */
    private int raftServiceCliServiceRpcThreadNum = 2;
    /** raft rpc请求timeout毫秒数 */
    private int rpcRequestTimeoutMs = 5000;

    //getter
    public String getDataDir() {
        return dataDir;
    }

    public String getAddress() {
        return address;
    }

    public String getClusterAddresses() {
        return clusterAddresses;
    }

    public String getServiceAddress() {
        return serviceAddress;
    }

    public RaftServiceFactory getRaftServiceFactory() {
        return raftServiceFactory;
    }

    public List<NodeStateChangeListener> getListeners() {
        return listeners;
    }

    public List<RaftGroupOptions> getRaftGroupOptionsList() {
        return raftGroupOptionsList;
    }

    public NodeOptionsCustomizer getNodeOptionsCustomizer() {
        return nodeOptionsCustomizer;
    }

    public int getRaftRpcThreadNum() {
        return raftRpcThreadNum;
    }

    public int getRaftCliServiceRpcThreadNum() {
        return raftCliServiceRpcThreadNum;
    }

    public int getRaftServiceRpcThreadNum() {
        return raftServiceRpcThreadNum;
    }

    public int getRaftServiceCliServiceRpcThreadNum() {
        return raftServiceCliServiceRpcThreadNum;
    }

    public int getRpcRequestTimeoutMs() {
        return rpcRequestTimeoutMs;
    }

    //-------------------------------------------------------builder
    public static Builder builder() {
        return new Builder();
    }

    /** builder **/
    public static class Builder {
        private final RaftServerOptions options = new RaftServerOptions();

        public Builder dataDir(String dataDir) {
            Preconditions.checkArgument(StringUtils.isNotBlank(dataDir), "dataDir must be not blank");
            options.dataDir = dataDir;
            return this;
        }

        public Builder address(String address) {
            Preconditions.checkArgument(StringUtils.isNotBlank(address), "address must be not blank");
            options.address = address;
            return this;
        }

        public Builder clusterAddresses(String clusterAddresses) {
            Preconditions.checkArgument(StringUtils.isNotBlank(clusterAddresses), "clusterAddresses must be not blank");
            options.clusterAddresses = clusterAddresses;
            return this;
        }

        public Builder serviceAddress(String serviceAddress) {
            Preconditions.checkArgument(StringUtils.isNotBlank(serviceAddress), "serviceAddress must be not blank");
            options.serviceAddress = serviceAddress;
            return this;
        }

        public Builder raftServiceFactory(RaftServiceFactory raftServiceFactory) {
            options.raftServiceFactory = raftServiceFactory;
            return this;
        }

        public Builder listeners(NodeStateChangeListener... listeners) {
            return listeners(Arrays.asList(listeners));
        }

        public Builder listeners(List<NodeStateChangeListener> listeners) {
            options.listeners.addAll(listeners);
            return this;
        }

        public Builder group(RaftGroupOptions raftGroupOptions) {
            return groups(Collections.singletonList(raftGroupOptions));
        }

        public Builder groups(List<RaftGroupOptions> raftGroupOptionsList) {
            options.raftGroupOptionsList.addAll(raftGroupOptionsList);
            return this;
        }

        public Builder nodeOptionsCustomizer(NodeOptionsCustomizer customizer) {
            options.nodeOptionsCustomizer = customizer;
            return this;
        }

        public Builder raftRpcThreadNum(int raftRpcThreadNum) {
            Preconditions.checkArgument(raftRpcThreadNum > 0, "raftRpcThreadNum must be greater than 0");
            options.raftRpcThreadNum = raftRpcThreadNum;
            return this;
        }

        public Builder raftCliServiceRpcThreadNum(int raftCliServiceRpcThreadNum) {
            Preconditions.checkArgument(raftCliServiceRpcThreadNum > 0, "raftCliServiceRpcThreadNum must be greater than 0");
            options.raftCliServiceRpcThreadNum = raftCliServiceRpcThreadNum;
            return this;
        }

        public Builder raftServiceRpcThreadNum(int raftServiceRpcThreadNum) {
            Preconditions.checkArgument(raftServiceRpcThreadNum > 0, "raftServiceRpcThreadNum must be greater than 0");
            options.raftServiceRpcThreadNum = raftServiceRpcThreadNum;
            return this;
        }

        public Builder raftServiceCliServiceRpcThreadNum(int raftServiceCliServiceRpcThreadNum) {
            Preconditions.checkArgument(raftServiceCliServiceRpcThreadNum > 0, "raftServiceCliServiceRpcThreadNum must be greater than 0");
            options.raftServiceCliServiceRpcThreadNum = raftServiceCliServiceRpcThreadNum;
            return this;
        }

        public Builder rpcRequestTimeoutMs(int rpcRequestTimeoutMs) {
            Preconditions.checkArgument(rpcRequestTimeoutMs > 0, "rpcRequestTimeoutMs must be greater than 0");
            options.rpcRequestTimeoutMs = rpcRequestTimeoutMs;
            return this;
        }

        public RaftServer build() {
            RaftServer raftServer = new RaftServer();
            raftServer.init(options);
            return raftServer;
        }
    }
}
