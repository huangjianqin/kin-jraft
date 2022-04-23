package org.kin.jraft;

import com.alipay.sofa.jraft.StateMachine;

/**
 * {@link com.alipay.sofa.jraft.StateMachine}工厂
 *
 * @author huangjianqin
 * @date 2021/11/7
 */
@FunctionalInterface
public interface StateMachineFactory{
    /**
     * 创建{@link com.alipay.sofa.jraft.StateMachine}实例并进行初始化
     *
     * @param raftServer raft server
     * @param raftGroup   raft group
     */
    AbstractStateMachine create(RaftServer raftServer, RaftGroup raftGroup);
}
