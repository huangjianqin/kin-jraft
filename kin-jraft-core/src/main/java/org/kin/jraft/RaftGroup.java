package org.kin.jraft;

import com.alipay.sofa.jraft.Closure;
import com.alipay.sofa.jraft.Node;
import com.alipay.sofa.jraft.RaftGroupService;
import com.alipay.sofa.jraft.Status;
import com.alipay.sofa.jraft.closure.ReadIndexClosure;
import com.alipay.sofa.jraft.entity.PeerId;
import com.alipay.sofa.jraft.entity.Task;
import com.alipay.sofa.jraft.error.RaftError;
import com.alipay.sofa.jraft.error.RemotingException;
import com.alipay.sofa.jraft.rpc.InvokeCallback;
import org.kin.framework.Closeable;
import org.kin.framework.log.LoggerOprs;
import org.kin.framework.utils.ExceptionUtils;

import java.nio.ByteBuffer;
import java.util.Objects;

/**
 * raft group
 *
 * @author huangjianqin
 * @date 2021/11/7
 */
@SuppressWarnings({"rawtypes", "unchecked"})
public final class RaftGroup implements Closeable, LoggerOprs {
    /** raft group id */
    private final String groupId;
    /** 所属{@link RaftServer} */
    private final RaftServer raftServer;
    /** raft service */
    private RaftGroupService raftGroupService;
    /** raft node */
    private Node node;
    /** {@link com.alipay.sofa.jraft.StateMachine}实现 */
    private AbstractStateMachine sm;
    private boolean stopped;

    public RaftGroup(String groupId, RaftServer raftServer) {
        this.groupId = groupId;
        this.raftServer = raftServer;
    }

    @Override
    public synchronized void close() {
        if (stopped) {
            return;
        }
        stopped = true;

        raftGroupService.shutdown();
        try {
            raftGroupService.join();
        } catch (InterruptedException e) {
            ExceptionUtils.throwExt(e);
        }
        info("raft group '{}` shutdown successfully", groupId);
    }

    /**
     * 当前node是否是leader
     */
    public boolean isLeader() {
        return getSm().isLeader();
    }

    /**
     * apply task
     * 只有在leader节点操作才会成功
     *
     * @param dataObj task data
     * @param closure task完成后的逻辑处理
     */
    public void applyTask(Object dataObj, Closure closure) {
        if (!isLeader()) {
            throw new NoLeaderException(groupId);
        }

        try {
            applyTask(ByteBuffer.wrap(RaftUtils.PROTOBUF.serialize(dataObj)), closure);
        } catch (Exception e) {
            error("fail to encode data", e);
            closure.run(new Status(RaftError.EINTERNAL, "fail to encode data".concat(ExceptionUtils.getExceptionDesc(e))));
        }
    }

    /**
     * apply task
     * 只有在leader节点操作才会成功
     *
     * @param data    task data bytes
     * @param closure task完成后的逻辑处理
     */
    public void applyTask(ByteBuffer data, Closure closure) {
        if (!isLeader()) {
            throw new NoLeaderException(groupId);
        }

        Task task = new Task();
        task.setData(data);
        task.setDone(closure);
        getNode().apply(task);
    }

    /**
     * 远程请求leader处理
     * @param dataObj task data
     * @param callback rpc invoke callback
     */
    public void invokeToLeader(Object dataObj, InvokeCallback callback) {
        PeerId leaderPeerId = raftServer.getLeader(groupId);
        if (Objects.isNull(leaderPeerId)) {
            throw new IllegalStateException(String.format("no leader for raft group '%s'", groupId));
        }
        try {
            raftServer.getCliClientService()
                    .getRpcClient().invokeAsync(leaderPeerId.getEndpoint(), dataObj, callback, raftServer.getRpcRequestTimeoutMs());
        } catch (InterruptedException | RemotingException e) {
            ExceptionUtils.throwExt(e);
        }
    }

    /**
     * read index查询
     * @param closure   read index查询成功后逻辑处理
     */
    public void readIndex(ReadIndexClosure closure) {
        readIndex(RaftUtils.EMPTY_BYTES, closure);
    }

    /**
     * read index查询
     * @param requestContext    查询参数bytes
     * @param closure   read index查询成功后逻辑处理
     */
    public void readIndex(byte[] requestContext, ReadIndexClosure closure) {
        node.readIndex(requestContext, closure);
    }


    //getter
    public Node getNode() {
        return node;
    }

    public <NW extends AbstractStateMachine> NW getSm() {
        return (NW) sm;
    }

    public boolean isStopped() {
        return stopped;
    }

    void setRaftGroupService(RaftGroupService raftGroupService) {
        this.raftGroupService = raftGroupService;
    }

    void setNode(Node node) {
        this.node = node;
    }

    void setSm(AbstractStateMachine sm) {
        this.sm = sm;
    }
}