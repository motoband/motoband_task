package com.motoband.task.impl.cmq;

import java.util.List;

import com.github.ltsopensource.core.logger.Logger;
import com.github.ltsopensource.core.logger.LoggerFactory;
import com.github.ltsopensource.tasktracker.Result;
import com.github.ltsopensource.tasktracker.runner.JobContext;
import com.github.ltsopensource.tasktracker.runner.JobRunner;
import com.motoband.manager.hardware.gps.GPSManager;
import com.motoband.model.QueueMonitorModel;

public class CMQ_WARNING implements JobRunner{
	protected static final Logger LOGGER = LoggerFactory.getLogger(CMQ_WARNING.class);
	@Override
	public Result run(JobContext arg0) throws Throwable {
		LOGGER.info("CMQ_WARNING is started");
		List<QueueMonitorModel> queueMonitorModels= GPSManager.getInstance().CmqVisibleMessageCount();
		LOGGER.info("CMQ_WARNING is finished");
		return null;
	}

}
