package com.motobang.task.impl.gps;

import com.github.ltsopensource.core.logger.Logger;
import com.github.ltsopensource.core.logger.LoggerFactory;
import com.github.ltsopensource.tasktracker.Result;
import com.github.ltsopensource.tasktracker.runner.JobContext;
import com.github.ltsopensource.tasktracker.runner.JobRunner;
import com.motoband.manager.hardware.gps.GPSManager;

/**
 * 定时部署
 * Created by junfei.Yang on 2020年12月4日.
 */
public class CMQ_TIMEING_MASTERCOUNT implements JobRunner{
	protected static final Logger LOGGER = LoggerFactory.getLogger(CMQ_TIMEING_MASTERCOUNT.class);
	public Result run(JobContext arg0) throws Throwable {
		LOGGER.info("CMQ_TIMEING_MASTERCOUNT is started");
		GPSManager.getInstance().changeCmqMasterCount();
		LOGGER.info("CMQ_TIMEING_MASTERCOUNT is finished");
		return null;
	}
}
