/*******************************************************************************
 * @(#)MyAtLeastOneSource.java 2021/9/9
 *
 * Copyright 2021 emrubik Group Ltd. All rights reserved.
 * EMRubik PROPRIETARY/CONFIDENTIAL. Use is subject to license terms.
 *******************************************************************************/
package com.atguigu.flink.apitest;

import org.apache.commons.io.Charsets;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;

import java.io.RandomAccessFile;

/**
 * 这里请补充该类型的简述说明
 *
 * @author <a href="mailto:person@emrubik.com">sunyj</a>
 * @version $$Revision 1.0 $$ 2021/9/9 13:46
 */
public class MyAtLeastOneSource extends RichParallelSourceFunction<String> implements CheckpointedFunction {

    private volatile boolean isRunning = true;

    private String path;

    private long offset = 0L;

    private transient ListState<Long> listState;

    public MyAtLeastOneSource(String path) {
        this.path = path;
    }

    @Override
    public void run(SourceContext<String> ctx) throws Exception {
        int indexOfThisSubtask = getRuntimeContext().getIndexOfThisSubtask();
        RandomAccessFile randomAccessFile = new RandomAccessFile(path + "/" + indexOfThisSubtask + ".txt","r");
        // 从指定位置读取
        randomAccessFile.seek(offset);
        while (isRunning){
            String line = randomAccessFile.readLine();
            if (StringUtils.isNotEmpty(line)){
                synchronized (ctx.getCheckpointLock()){
                    line = new String(line.getBytes(Charsets.ISO_8859_1), Charsets.UTF_8);
                    offset = randomAccessFile.getFilePointer();
                    ctx.collect(indexOfThisSubtask+".txt: " + line);
                }
            } else {
                Thread.sleep(1000);
            }
        }

    }

    @Override
    public void cancel() {
        isRunning = false;
    }


    /**
     * 周期性的执行，
     * @param context
     * @throws Exception
     */
    @Override
    public void snapshotState(FunctionSnapshotContext context) throws Exception {
         // 定期的更新opratorState
        System.out.println("任务编号:["+getRuntimeContext().getIndexOfThisSubtask()+"]进行snapshotState");
        listState.clear();
        listState.add(offset);
    }

    /**
     * 初始化或者恢复状态执行一次，在run执行之前执行一次
     * @param context
     * @throws Exception
     */
    @Override
    public void initializeState(FunctionInitializationContext context) throws Exception {
        System.out.println("任务编号:["+getRuntimeContext().getIndexOfThisSubtask()+"]初始化");
        ListStateDescriptor<Long> stateDescriptor = new ListStateDescriptor<Long>("offset-state",Long.class);
        listState = context.getOperatorStateStore().getListState(stateDescriptor);
        if (context.isRestored()){
            Iterable<Long> iterable = listState.get();
            for (Long along: iterable){
                offset = along;
            }
        }
    }
}
