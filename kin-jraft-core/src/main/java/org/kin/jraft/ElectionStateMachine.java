package org.kin.jraft;

/**
 * do nothing的{@link com.alipay.sofa.jraft.StateMachine}实现
 * 一般用于基于raft实现选举时使用
 *
 * @author huangjianqin
 * @date 2022/4/20
 */
public final class ElectionStateMachine extends AbstractStateMachine{

    public ElectionStateMachine(RaftServer raftServer, RaftGroup raftGroup) {
        super(raftServer, raftGroup);
    }

    @Override
    protected String getGroup() {
        return "election";
    }
}
