package org.kin.jraft.springboot;

import com.alipay.sofa.jraft.core.ElectionPriority;
import com.alipay.sofa.jraft.option.ReadOnlyOption;
import com.alipay.sofa.jraft.storage.SnapshotThrottle;
import com.alipay.sofa.jraft.util.Utils;
import com.codahale.metrics.MetricRegistry;
import org.kin.framework.utils.StringUtils;
import org.kin.jraft.*;
import org.springframework.boot.context.properties.ConfigurationProperties;

import java.util.ArrayList;
import java.util.List;

/**
 * @author huangjianqin
 * @date 2021/11/24
 */
@SuppressWarnings("rawtypes")
@ConfigurationProperties("kin.jraft.server")
public class RaftServerProperties {
    /** 数据存储目录 */
    private String dataDir;
    /** local node绑定的ip:port */
    private String address;
    /** raft node集群的ip:port,ip:port,ip:port */
    private String clusterAddresses;
    /** ip:port, raft service绑定地址 */
    private String serviceAddress;
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

    public void fillRaftServerOptions(RaftServerOptions.Builder builder){
        builder.dataDir(dataDir);
        builder.address(address);
        builder.clusterAddresses(clusterAddresses);
        if (StringUtils.isNotBlank(serviceAddress)) {
            builder.serviceAddress(serviceAddress);
        }
        builder.raftRpcThreadNum(raftRpcThreadNum);
        builder.raftCliServiceRpcThreadNum(raftCliServiceRpcThreadNum);
        builder.raftServiceRpcThreadNum(raftServiceRpcThreadNum);
        builder.raftServiceCliServiceRpcThreadNum(raftServiceCliServiceRpcThreadNum);
        builder.rpcRequestTimeoutMs(rpcRequestTimeoutMs);
    }

    //setter && getter
    public String getDataDir() {
        return dataDir;
    }

    public void setDataDir(String dataDir) {
        this.dataDir = dataDir;
    }

    public String getAddress() {
        return address;
    }

    public void setAddress(String address) {
        this.address = address;
    }

    public String getClusterAddresses() {
        return clusterAddresses;
    }

    public void setClusterAddresses(String clusterAddresses) {
        this.clusterAddresses = clusterAddresses;
    }

    public String getServiceAddress() {
        return serviceAddress;
    }

    public void setServiceAddress(String serviceAddress) {
        this.serviceAddress = serviceAddress;
    }

    public int getRaftRpcThreadNum() {
        return raftRpcThreadNum;
    }

    public void setRaftRpcThreadNum(int raftRpcThreadNum) {
        this.raftRpcThreadNum = raftRpcThreadNum;
    }

    public int getRaftCliServiceRpcThreadNum() {
        return raftCliServiceRpcThreadNum;
    }

    public void setRaftCliServiceRpcThreadNum(int raftCliServiceRpcThreadNum) {
        this.raftCliServiceRpcThreadNum = raftCliServiceRpcThreadNum;
    }

    public int getRaftServiceRpcThreadNum() {
        return raftServiceRpcThreadNum;
    }

    public void setRaftServiceRpcThreadNum(int raftServiceRpcThreadNum) {
        this.raftServiceRpcThreadNum = raftServiceRpcThreadNum;
    }

    public int getRaftServiceCliServiceRpcThreadNum() {
        return raftServiceCliServiceRpcThreadNum;
    }

    public void setRaftServiceCliServiceRpcThreadNum(int raftServiceCliServiceRpcThreadNum) {
        this.raftServiceCliServiceRpcThreadNum = raftServiceCliServiceRpcThreadNum;
    }

    public int getRpcRequestTimeoutMs() {
        return rpcRequestTimeoutMs;
    }

    public void setRpcRequestTimeoutMs(int rpcRequestTimeoutMs) {
        this.rpcRequestTimeoutMs = rpcRequestTimeoutMs;
    }
}
