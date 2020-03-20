package com.motobang.task.impl.push;

import com.github.ltsopensource.tasktracker.runner.InterruptibleJobRunner;

/**
 * @author Robert HG (254963746@qq.com) on 2/21/16.
 */
public class TestInterruptorJobRunner extends TestJob implements InterruptibleJobRunner {

    @Override
    public void interrupt() {
        System.out.println("我设置停止标识");
    }
}
