package org.kin.jraft;

import com.alipay.sofa.jraft.option.NodeOptions;

/**
 * 提供接口给使用者自定义raft node配置
 * @author huangjianqin
 * @date 2022/4/20
 */
@FunctionalInterface
public interface NodeOptionsCustomizer {
    /**
     * 供使用者自定义raft node配置
     * @param nodeOpts  raft node配置
     */
    void customize(NodeOptions nodeOpts);
}
