package org.kin.jraft;

import org.kin.framework.concurrent.ExecutionContext;
import org.kin.serialization.Serialization;
import org.kin.serialization.protobuf.ProtobufSerialization;

import java.nio.ByteBuffer;
import java.util.concurrent.ScheduledExecutorService;

/**
 * @author huangjianqin
 * @date 2021/11/14
 */
public final class RaftUtils {
    /** {@link com.alipay.sofa.jraft.entity.Task#setData(ByteBuffer)} 默认使用的序列化方法 */
    public static final Serialization PROTOBUF = new ProtobufSerialization();
    /** 空bytes */
    public static final byte[] EMPTY_BYTES = new byte[0];

    private RaftUtils() {
    }
}
