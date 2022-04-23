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
    /** 所属raft server */
    protected final RaftServer raftServer;

    protected AbstractRaftService(RaftServer raftServer) {
        this.raftServer = raftServer;
    }
}
