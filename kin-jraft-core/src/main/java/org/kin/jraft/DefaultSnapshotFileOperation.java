package org.kin.jraft;

import org.apache.commons.io.FileUtils;
import org.apache.commons.lang.StringUtils;
import org.kin.framework.log.LoggerOprs;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;

/**
 * 默认的快照加载逻辑
 *
 * @author huangjianqin
 * @date 2021/11/14
 */
final class DefaultSnapshotFileOperation implements SnapshotFileOperation<Object>, LoggerOprs {
    /** 单例 */
    static final DefaultSnapshotFileOperation INSTANCE = new DefaultSnapshotFileOperation();

    @Override
    public boolean save(String path, Object obj) {
        try {
            FileUtils.writeStringToFile(new File(path), obj.toString(), StandardCharsets.UTF_8);
            return true;
        } catch (IOException e) {
            error("fail to save snapshot", e);
            return false;
        }
    }

    @Override
    public Object load(String path) throws IOException {
        String s = FileUtils.readFileToString(new File(path), StandardCharsets.UTF_8);
        if (!StringUtils.isBlank(s)) {
            return Long.parseLong(s);
        }
        throw new IOException("fail to load snapshot from " + path + ",content: " + s);
    }
}
