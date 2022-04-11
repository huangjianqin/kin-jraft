package org.kin.jraft;

import com.alipay.sofa.jraft.Status;
import com.alipay.sofa.jraft.entity.Task;
import com.alipay.sofa.jraft.error.RaftError;
import org.apache.commons.lang.StringUtils;

import java.nio.ByteBuffer;

/**
 * @author huangjianqin
 * @date 2021/11/14
 */
public abstract class AbstractRaftService implements RaftService {
    protected final RaftServer raftServer;

    protected AbstractRaftService(RaftServer raftServer) {
        this.raftServer = raftServer;
    }

    /**
     * 当前node是否是leader
     */
    protected boolean isLeader() {
        return raftServer.getSm().isLeader();
    }

    /**
     * 处理没有leader异常
     */
    protected void handlerNotLeaderError(AbstractClosure<?> closure) {
        closure.failure("not leader.", raftServer.getNode().getLeaderId().toString());
        closure.run(new Status(RaftError.EPERM, "not leader"));
    }

    /**
     * leader节点apply task
     *
     * @param dataObj task data
     */
    protected void applyTask(Object dataObj, AbstractClosure<?> closure) {
        try {
            applyTask(ByteBuffer.wrap(RaftUtils.PROTOBUF.serialize(dataObj)), closure);
        } catch (Exception e) {
            String errorMsg = "fail to encode data";
            error(errorMsg, e);
            closure.failure(errorMsg, StringUtils.EMPTY);
            closure.run(new Status(RaftError.EINTERNAL, errorMsg));
        }
    }

    /**
     * leader节点apply task
     */
    protected void applyTask(ByteBuffer data, AbstractClosure<?> closure) {
        if (!isLeader()) {
            handlerNotLeaderError(closure);
            return;
        }

        Task task = new Task();
        task.setData(data);
        task.setDone(closure);
        raftServer.getNode().apply(task);
    }

}
