package com.motobang.task.impl.gps;

import java.util.ArrayList;

import com.alibaba.fastjson.JSON;
import com.github.ltsopensource.core.domain.Action;
import com.github.ltsopensource.core.logger.Logger;
import com.github.ltsopensource.core.logger.LoggerFactory;
import com.github.ltsopensource.tasktracker.Result;
import com.github.ltsopensource.tasktracker.runner.InterruptibleJobRunner;
import com.github.ltsopensource.tasktracker.runner.JobContext;
import com.motoband.manager.hardware.gps.GPSManager;
import com.motoband.model.QueueMonitorModel;

/**
 * cmq消息可见消息数量告警
 * Created by junfei.Yang on 2020年12月4日.
 */
public class CMQ_WARNING implements InterruptibleJobRunner{
	protected static final Logger LOGGER = LoggerFactory.getLogger(CMQ_WARNING.class);
	@Override
	public Result run(JobContext arg0) throws Throwable {
		LOGGER.info("CMQ_WARNING is started");
		ArrayList<QueueMonitorModel> res=GPSManager.getInstance().CmqVisibleMessageCount();
		LOGGER.info("CMQ_WARNING is finished");
		return new Result(Action.EXECUTE_SUCCESS, JSON.toJSONString(res)) ;
	}
	@Override
	public void interrupt() {
		LOGGER.info("结束处理...............");
	}

}
