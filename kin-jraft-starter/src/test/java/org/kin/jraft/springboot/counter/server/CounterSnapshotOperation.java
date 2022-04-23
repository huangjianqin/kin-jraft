package org.kin.jraft.springboot.counter.server;

import org.apache.commons.io.FileUtils;
import org.apache.commons.lang.StringUtils;
import org.kin.framework.log.LoggerOprs;
import org.kin.jraft.SnapshotContext;
import org.kin.jraft.SnapshotOperation;
import org.kin.jraft.springboot.counter.server.CounterStateMachine;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.function.BiConsumer;

/**
 * @author huangjianqin
 * @date 2021/11/14
 */
public class CounterSnapshotOperation implements SnapshotOperation<CounterStateMachine>, LoggerOprs {
    private static final String SNAPSHOT_FILE_NAME = "counter";

    @Override
    public void save(CounterStateMachine stateMachine, SnapshotContext context, BiConsumer<Boolean, Throwable> callFinally) {
        try {
            File file = new File(context.getPath().concat(File.separator).concat(SNAPSHOT_FILE_NAME));
            if (!file.exists()) {
                file.createNewFile();
            }
            FileUtils.writeStringToFile(file, stateMachine.getValue() + "", StandardCharsets.UTF_8);
            callFinally.accept(true, null);
        } catch (IOException e) {
            error("fail to save snapshot", e);
            callFinally.accept(false, e);
        }
    }

    @Override
    public boolean load(CounterStateMachine stateMachine, SnapshotContext context) {
        String s;
        try {
            File file = new File(context.getPath().concat(File.separator).concat(SNAPSHOT_FILE_NAME));
            if(!file.exists()){
                return true;
            }
            s = FileUtils.readFileToString(file, StandardCharsets.UTF_8);
            if (!StringUtils.isBlank(s)) {
                stateMachine.setValue(Long.parseLong(s));
            }
            info("load snapshot >>> " + s);
            return true;
        } catch (IOException e) {
            error("fail to load snapshot", e);
        }
        return false;
    }
}
