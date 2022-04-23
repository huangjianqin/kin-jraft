package org.kin.jraft;

/**
 * 监听raft node状态变化listener
 *
 * @author huangjianqin
 * @date 2021/11/7
 */
public interface NodeStateChangeListener {
    /**
     * 当前节点成为leader时触发
     *
     * @param group raft group
     * @param term 新leader的term
     */
    default void onBecomeLeader(String group, long term) {
    }

    /**
     * 当前节点step down时触发
     *
     * @param group raft group
     * @param oldTerm 原leader的term
     */
    default void onStepDown(String group, long oldTerm) {
    }
}
