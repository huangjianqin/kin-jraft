package org.kin.jraft;

/**
 * 找不到raft group leader异常
 *
 * @author huangjianqin
 * @date 2022/4/23
 */
public class NoLeaderException extends RuntimeException {
    private static final long serialVersionUID = 7579582866511976125L;

    public NoLeaderException(String group) {
        super("raft group (" + group + ") did not find the leader node");
    }
}