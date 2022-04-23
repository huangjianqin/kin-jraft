package org.kin.jraft;

import java.io.IOException;
import java.util.function.BiConsumer;

/**
 * 快照操作
 * @author huangjianqin
 * @date 2021/11/14
 */
public interface SnapshotOperation<T extends AbstractStateMachine> {
    SnapshotOperation<?> DO_NOTHING = new SnapshotOperation<AbstractStateMachine>() {
        @Override
        public void save(AbstractStateMachine stateMachine, SnapshotContext context, BiConsumer<Boolean, Throwable> callFinally) {
            //do nothing
            callFinally.accept(true, null);
        }

        @Override
        public boolean load(AbstractStateMachine stateMachine, SnapshotContext context) {
            //do nothing
            return true;
        }
    };

    /**
     * 快照保存
     * @param stateMachine  所属stateMachine
     * @param context   快照上下文
     * @param callFinally   快照保存后回调
     */
    void save(T stateMachine, SnapshotContext context, BiConsumer<Boolean, Throwable> callFinally);

    /**
     * 快照加载
     * @param stateMachine  所属stateMachine
     * @param context   快照上下文
     * @return  快照是否加载
     */
    boolean load(T stateMachine, SnapshotContext context);
}
