package com.motobang.task.impl.push;

import com.github.ltsopensource.tasktracker.Result;
import com.github.ltsopensource.tasktracker.runner.JobContext;
import com.github.ltsopensource.tasktracker.runner.JobRunner;

public class TestJob  implements JobRunner{

	@Override
	public Result run(JobContext jobContext) throws Throwable {
		while (true) {
			try {
				System.out.println("111111111");
				Thread.sleep(1*1000);
			} catch (Exception e) {
				break;
			}
		
		}
		return null;
	}

}
