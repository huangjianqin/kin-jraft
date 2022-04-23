package org.kin.jraft;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * raft group配置
 *
 * @author huangjianqin
 * @date 2021/11/7
 */
public final class RaftGroupOptions {
    /** raft group id */
    private String groupId;
    /** 自定义{@link com.alipay.sofa.jraft.StateMachine}实现类构建逻辑 */
    private StateMachineFactory stateMachineFactory;
    /** 快照文件操作, 默认不写快照 */
    private SnapshotOperation<?> snapshotOperation = SnapshotOperation.DO_NOTHING;
    /** {@link NodeStateChangeListener} list */
    private final List<NodeStateChangeListener> listeners = new ArrayList<>();
    /** 自定义raft node配置 */
    private NodeOptionsCustomizer nodeOptionsCustomizer;

    //getter
    public String getGroupId() {
        return groupId;
    }

    public StateMachineFactory getStateMachineFactory() {
        return stateMachineFactory;
    }

    public SnapshotOperation<?> getSnapshotFileOperation() {
        return snapshotOperation;
    }

    public List<NodeStateChangeListener> getListeners() {
        return listeners;
    }

    public NodeOptionsCustomizer getNodeOptionsCustomizer() {
        return nodeOptionsCustomizer;
    }

    //-------------------------------------------------------builder
    public static Builder builder(String groupId) {
        Builder builder = new Builder();
        builder.options.groupId = groupId;
        return builder;
    }

    /** builder **/
    public static class Builder {
        private final RaftGroupOptions options = new RaftGroupOptions();

        public Builder listeners(List<NodeStateChangeListener> listeners) {
            options.listeners.addAll(listeners);
            return this;
        }

        public Builder listeners(NodeStateChangeListener... listeners) {
            return listeners(Arrays.asList(listeners));
        }

        public Builder stateMachineFactory(StateMachineFactory stateMachineFactory) {
            options.stateMachineFactory = stateMachineFactory;
            return this;
        }

        public Builder snapshotFileOperation(SnapshotOperation<?> snapshotOperation) {
            options.snapshotOperation = snapshotOperation;
            return this;
        }

        public Builder nodeOptionsCustomizer(NodeOptionsCustomizer customizer) {
            options.nodeOptionsCustomizer = customizer;
            return this;
        }

        public RaftGroupOptions build() {
            return options;
        }
    }
}
