package org.kin.jraft.counter;

import com.alipay.sofa.jraft.Iterator;
import com.alipay.sofa.jraft.Status;
import org.kin.jraft.AbstractStateMachine;
import org.kin.jraft.RaftGroup;
import org.kin.jraft.RaftServer;
import org.kin.jraft.RaftUtils;

import java.nio.ByteBuffer;
import java.util.concurrent.atomic.AtomicLong;

/**
 * @author huangjianqin
 * @date 2021/11/14
 */
public class CounterStateMachine extends AbstractStateMachine {
    /**
     * Counter value
     */
    private final AtomicLong value = new AtomicLong(0);

    public CounterStateMachine(RaftServer raftServer, RaftGroup raftGroup) {
        super(raftServer, raftGroup);
    }

    @Override
    public void onApply(Iterator iterator) {
        while (iterator.hasNext()) {
            long current = 0;
            CounterOperation counterOperation = null;

            CounterClosure closure = null;
            if (iterator.done() != null) {
                // This task is applied by this node, get value from closure to avoid additional parsing.
                // fast
                closure = (CounterClosure) iterator.done();
                counterOperation = closure.getOperation();
            } else {
                // 手动序列化
                ByteBuffer data = iterator.getData();
                try {
                    counterOperation = RaftUtils.PROTOBUF.deserialize(data.array(), CounterOperation.class);
                } catch (Exception e) {
                    error("fail to decode IncrementAndGetRequest", e);
                }
            }
            //不同操作类型, 不同操作
            if (counterOperation != null) {
                switch (counterOperation.getOp()) {
                    //get
                    case CounterOperation.GET:
                        current = value.get();
                        info("Get value={} at logIndex={}", current, iterator.getIndex());
                        break;

                    //alter
                    case CounterOperation.INCREMENT:
                        long delta = counterOperation.getDelta();
                        long prev = value.get();
                        current = value.addAndGet(delta);
                        info("Added value={} by delta={} at logIndex={}, current = {}", prev, delta, iterator.getIndex(), current);
                        break;
                }

                if (closure != null) {
                    //操作成功
                    closure.success(current);
                    closure.run(Status.OK());
                }
            }
            iterator.next();
        }
    }

    @Override
    protected String getGroup() {
        return Constants.GROUP_ID;
    }

    public long getValue() {
        return this.value.get();
    }

    public void setValue(long v){
        this.value.set(v);
    }
}
