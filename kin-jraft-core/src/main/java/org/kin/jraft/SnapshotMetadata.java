package org.kin.jraft;

import java.util.Properties;

/**
 * snapshot元数据
 *
 * @author huangjianqin
 * @date 2022/4/23
 */
public class SnapshotMetadata {
    public static final SnapshotMetadata EMPTY = new SnapshotMetadata();

    /** 元数据key-value */
    private final Properties metadata;

    public SnapshotMetadata() {
        this.metadata = new Properties();
    }

    public SnapshotMetadata(Properties properties) {
        this.metadata = properties;
    }

    /**
     * 添加元数据信息
     * @param key   元数据key
     * @param value 元数据value
     * @return  this
     */
    public SnapshotMetadata append(Object key, Object value) {
        metadata.put(key, value);
        return this;
    }

    /**
     * 获取元数据信息
     * @param key   元数据key
     * @return  元数据value
     */
    public Object get(String key) {
        return metadata.getProperty(key);
    }

    //getter
    public Properties getMetadata() {
        return metadata;
    }

    @Override
    public String toString() {
        return "SnapshotFileMetadata{" +
                "metadata=" + metadata +
                '}';
    }
}
