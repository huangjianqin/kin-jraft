package org.kin.jraft.counter.processor;

import com.alipay.sofa.jraft.Status;
import com.alipay.sofa.jraft.rpc.RpcContext;
import com.alipay.sofa.jraft.rpc.RpcProcessor;
import org.kin.jraft.counter.CounterClosure;
import org.kin.jraft.counter.CounterRaftService;
import org.kin.jraft.counter.message.IncrementAndGetRequest;

/**
 * @author huangjianqin
 * @date 2021/11/14
 */
public class IncrementAndGetRequestProcessor implements RpcProcessor<IncrementAndGetRequest> {

    private final CounterRaftService counterService;

    public IncrementAndGetRequestProcessor(CounterRaftService counterService) {
        super();
        this.counterService = counterService;
    }

    @Override
    public void handleRequest(RpcContext rpcCtx, IncrementAndGetRequest request) {
        CounterClosure closure = new CounterClosure() {
            @Override
            public void run(Status status) {
                rpcCtx.sendResponse(getResponse());
            }
        };

        this.counterService.incrementAndGet(request.getDelta(), closure);
    }

    @Override
    public String interest() {
        return IncrementAndGetRequest.class.getName();
    }
}