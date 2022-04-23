package org.kin.jraft;

import com.alipay.sofa.jraft.Closure;
import com.alipay.sofa.jraft.Iterator;
import com.alipay.sofa.jraft.Status;
import com.alipay.sofa.jraft.core.StateMachineAdapter;
import com.alipay.sofa.jraft.entity.LocalFileMetaOutter;
import com.alipay.sofa.jraft.error.RaftError;
import com.alipay.sofa.jraft.error.RaftException;
import com.alipay.sofa.jraft.storage.snapshot.SnapshotReader;
import com.alipay.sofa.jraft.storage.snapshot.SnapshotWriter;
import com.google.protobuf.ZeroByteStringHelper;
import org.kin.framework.log.LoggerOprs;
import org.kin.framework.utils.ExceptionUtils;
import org.kin.framework.utils.JSON;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.BiConsumer;

/**
 * 监听当前节点状态变化的{@link com.alipay.sofa.jraft.StateMachine}实现
 *
 * @author huangjianqin
 * @date 2021/11/7
 */
@SuppressWarnings("rawtypes")
public abstract class AbstractStateMachine extends StateMachineAdapter implements LoggerOprs {
    /** leader任期号 */
    private final AtomicLong leaderTerm = new AtomicLong(-1L);
    /** 注册的{@link NodeStateChangeListener}实例 */
    private final List<NodeStateChangeListener> listeners = new CopyOnWriteArrayList<>();
    /** 所属raft server */
    private final RaftServer raftServer;
    /** 所属raft group */
    private final RaftGroup raftGroup;
    /** 快照操作逻辑 */
    private SnapshotOperation snapshotOperation;

    public AbstractStateMachine(RaftServer raftServer, RaftGroup raftGroup) {
        this.raftServer = raftServer;
        this.raftGroup = raftGroup;
    }

    protected AbstractStateMachine(RaftServer raftServer, RaftGroup raftGroup, List<NodeStateChangeListener> listeners) {
        this(raftServer, raftGroup);
        this.listeners.addAll(listeners);
    }

    void init(SnapshotOperation snapshotOperation, Collection<NodeStateChangeListener> listeners){
        this.snapshotOperation = snapshotOperation;
        this.listeners.addAll(listeners);
    }

    @Override
    public void onApply(Iterator iterator) {
        // do nothing
        while (iterator.hasNext()) {
            info("apply with term: {} and index: {}. ", iterator.getTerm(), iterator.getIndex());
            iterator.next();
        }
    }

    @Override
    public final void onLeaderStart(long term) {
        super.onLeaderStart(term);
        leaderTerm.set(term);
        for (NodeStateChangeListener listener : listeners) {
            try {
                listener.onBecomeLeader(getGroup(), term);
            } catch (Exception e) {
                error("listener {} encounter error, {}", listener, e);
            }
        }
    }

    @Override
    public final void onLeaderStop(Status status) {
        super.onLeaderStop(status);
        long oldTerm = leaderTerm.get();
        leaderTerm.set(-1L);
        for (NodeStateChangeListener listener : listeners) {
            try {
                listener.onStepDown(getGroup(), oldTerm);
            } catch (Exception e) {
                error("listener {} encounter error, {}", listener, e);
            }
        }
    }

    @SuppressWarnings("unchecked")
    @Override
    public final void onSnapshotSave(SnapshotWriter writer, Closure done) {
        //snapshot上下文
        SnapshotContext context = new SnapshotContext(writer.getPath());
        //snapshot save后回调
        BiConsumer<Boolean, Throwable> callFinally = (result, t) -> {
            boolean allFileAddResult = true;
            for (Map.Entry<String, SnapshotMetadata> entry : context.listFiles().entrySet()) {
                String fileName = entry.getKey();
                SnapshotMetadata metadata = entry.getValue();
                try {
                    allFileAddResult &= writer.addFile(fileName, constructLocalFileMeta(metadata));
                } catch (Exception e) {
                    allFileAddResult = false;
                    ExceptionUtils.throwExt(e);
                }
            }

            //所有snapshot写入成功, 才算成功
            Status status = result && allFileAddResult ? Status.OK() :
                    new Status(RaftError.EIO, "fail to save snapshot at %s, error is %s", writer.getPath(), t == null ? "" : t.getMessage());
            done.run(status);
        };
        //save
        snapshotOperation.save(this, context, callFinally);
    }

    /**
     * 构建{@link LocalFileMetaOutter.LocalFileMeta}
     */
    private LocalFileMetaOutter.LocalFileMeta constructLocalFileMeta(SnapshotMetadata metadata) {
        return metadata == null ? null : LocalFileMetaOutter.LocalFileMeta.newBuilder().setUserMeta(ZeroByteStringHelper.wrap(JSON.writeBytes(metadata))).build();
    }

    @Override
    public void onError(RaftException e) {
        error("statemachine encounter error: {}", e);
    }

    @SuppressWarnings("unchecked")
    @Override
    public final boolean onSnapshotLoad(SnapshotReader reader) {
        //snapshot上下文
        SnapshotContext context = new SnapshotContext(reader.getPath());
        //读取snapshot metadata
        for (String fileName : reader.listFiles()) {
            LocalFileMetaOutter.LocalFileMeta meta = (LocalFileMetaOutter.LocalFileMeta) reader.getFileMeta(fileName);
            byte[] bytes = meta.getUserMeta().toByteArray();
            SnapshotMetadata metadata = SnapshotMetadata.EMPTY;
            if (bytes != null && bytes.length > 0) {
                metadata = JSON.read(bytes, SnapshotMetadata.class);
            }

            context.addFile(fileName, metadata);
        }
        //load
        return snapshotOperation.load(this, context);
    }

    /**
     * 返回raft group id
     *
     * @return raft group id
     */
    protected abstract String getGroup();

    /**
     * 当前节点是否是leader
     */
    public boolean isLeader() {
        return leaderTerm.get() > 0;
    }
}
