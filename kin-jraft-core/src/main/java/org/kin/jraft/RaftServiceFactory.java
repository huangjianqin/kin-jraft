package org.kin.jraft;

import com.alipay.sofa.jraft.rpc.RpcServer;

/**
 * raft service工厂接口
 *
 * @author huangjianqin
 * @date 2021/11/7
 */
@FunctionalInterface
public interface RaftServiceFactory {
    /**
     * 创建raft service实例并进行初始化(比如注册方法监听接口)
     *
     * @param raftServer multi group raft node server
     * @param rpcServer raft service绑定的rpc server
     */
    RaftService create(RaftServer raftServer, RpcServer rpcServer);
}
