package org.kin.jraft.springboot.counter.server;

import com.alipay.sofa.jraft.Status;
import com.alipay.sofa.jraft.closure.ReadIndexClosure;
import com.alipay.sofa.jraft.error.RaftError;
import com.alipay.sofa.jraft.rpc.InvokeCallback;
import org.kin.jraft.AbstractRaftService;
import org.kin.jraft.RaftGroup;
import org.kin.jraft.RaftServer;
import org.kin.jraft.springboot.counter.message.IncrementAndGetRequest;
import org.kin.jraft.springboot.counter.message.ValueResponse;

import java.util.Objects;
import java.util.concurrent.Executor;

/**
 * @author huangjianqin
 * @date 2021/11/14
 */
public class CounterRaftServiceImpl extends AbstractRaftService implements CounterRaftService {
    private final RaftGroup raftGroup;

    public CounterRaftServiceImpl(RaftServer raftServer) {
        super(raftServer);
        this.raftGroup = raftServer.getRaftGroup(Constants.GROUP_ID);
    }

    private long getValue() {
        CounterStateMachine sm = raftGroup.getSm();
        return sm.getValue();
    }

    @Override
    public void get(boolean readOnlySafe, CounterClosure closure) {
        if (!readOnlySafe) {
            closure.success(getValue());
            closure.run(Status.OK());
            return;
        }

        //查询, 先尝试走read index
        raftGroup.readIndex(new ReadIndexClosure() {
            @Override
            public void run(Status status, long index, byte[] reqCtx) {
                if (status.isOk()) {
                    //成功则返回
                    closure.success(getValue());
                    closure.run(Status.OK());
                    return;
                }

                //不然回退到, 走raft log
                CounterOperation dataObj = CounterOperation.createGet();
                if (raftGroup.isLeader()) {
                    //leader, 直接走raft log
                    debug("fail to get value with 'ReadIndex': {}, try to applying to the state machine.", status);
                    raftGroup.applyTask(dataObj, closure);
                } else {
                    //follower, 则走remote raft log
                    raftGroup.reqLeaderAsync(dataObj, new InvokeCallback() {
                        @Override
                        public void complete(Object o, Throwable ex) {
                            if (Objects.nonNull(ex)) {
                                closure.run(new Status(RaftError.UNKNOWN, ex.getMessage()));
                                return;
                            }
                            closure.setResponse((ValueResponse) o);
                            closure.run(Status.OK());
                        }

                        @Override
                        public Executor executor() {
                            return raftServer.getRaftCliServiceRpcExecutor();
                        }
                    });
                }
            }
        });
    }

    @Override
    public void incrementAndGet(long delta, CounterClosure closure) {
        CounterOperation operation = CounterOperation.createIncrement(delta);
        closure.setOperation(operation);
        if(raftGroup.isLeader()){
            //leader, 直接raft log
            raftGroup.applyTask(operation, closure);
        }
        else{
            //follower, remote raft log
            IncrementAndGetRequest request = new IncrementAndGetRequest();
            request.setDelta(delta);
            raftGroup.reqLeaderAsync(request, new InvokeCallback() {
                @Override
                public void complete(Object o, Throwable ex) {
                    if (Objects.nonNull(ex)) {
                        closure.run(new Status(RaftError.UNKNOWN, ex.getMessage()));
                        return;
                    }
                    closure.setResponse((ValueResponse) o);
                    closure.run(Status.OK());
                }

                @Override
                public Executor executor() {
                    return raftServer.getRaftCliServiceRpcExecutor();
                }
            });
        }
    }

    @Override
    public void close() {
        //do nothing
    }
}
