package org.kin.jraft;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

/**
 * 快照上下问, 保存和加载快照时需要用到
 * @author huangjianqin
 * @date 2022/4/23
 */
public class SnapshotContext {
    /** snapshot目录 */
    private final String path;
    /** key -> snapshot文件名, value -> snapshot元数据 */
    private final Map<String, SnapshotMetadata> metadataMap = new HashMap<>();

    public SnapshotContext(String path) {
        this.path = path;
    }

    /**
     * 添加快照文件
     * @param fileName  快照文件名
     */
    public void addFile(String fileName) {
        metadataMap.put(fileName, new SnapshotMetadata().append("file-name", fileName));
    }

    /**
     * 添加快照文件
     * @param fileName  快照文件名
     * @param metadata  快照元数据
     */
    public void addFile(String fileName,  SnapshotMetadata metadata) {
        metadataMap.put(fileName, metadata);
    }

    /**
     * 移除快照文件
     */
    public void removeFile(final String fileName) {
        metadataMap.remove(fileName);
    }

    /**
     * 获取快照元数据
     * @param fileName  快照文件名
     * @return  {@link SnapshotMetadata}实例
     */
    public SnapshotMetadata getFileMeta(String fileName) {
        return metadataMap.get(fileName);
    }

    /**
     * @return  所有快照元数据
     */
    public Map<String, SnapshotMetadata> listFiles() {
        return Collections.unmodifiableMap(metadataMap);
    }

    //getter
    public String getPath() {
        return path;
    }
}
