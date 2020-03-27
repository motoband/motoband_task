package com.motobang.task.impl.vote;

import com.github.ltsopensource.tasktracker.Result;
import com.github.ltsopensource.tasktracker.runner.JobContext;
import com.github.ltsopensource.tasktracker.runner.JobRunner;
import com.motoband.common.Consts;
import com.motoband.manager.RedisManager;
import com.motoband.manager.vote.VoteManager;

public class VOTE_CLOSE implements JobRunner {

	@Override
	public Result run(JobContext jobContext) throws Throwable {
		String voteid=jobContext.getJob().getParam("voteid");
		RedisManager.getInstance().hset(Consts.REDIS_SCHEME_NEWS, voteid+VoteManager.VOTE_INFO, "status","1");
		return null;
	}

}
