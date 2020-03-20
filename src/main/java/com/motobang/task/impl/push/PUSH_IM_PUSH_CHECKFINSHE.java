package com.motobang.task.impl.push;

import java.util.HashMap;
import java.util.Map;

import com.alibaba.fastjson.JSON;
import com.github.ltsopensource.core.domain.Job;
import com.github.ltsopensource.core.logger.Logger;
import com.github.ltsopensource.core.logger.LoggerFactory;
import com.github.ltsopensource.tasktracker.Result;
import com.github.ltsopensource.tasktracker.runner.JobContext;
import com.github.ltsopensource.tasktracker.runner.JobRunner;
import com.motoband.manager.UserManager;

public class PUSH_IM_PUSH_CHECKFINSHE implements JobRunner {
    protected static final Logger LOGGER = LoggerFactory.getLogger(PUSH_IM_PUSH_CHECKFINSHE.class);

	@Override
	public Result run(JobContext jobContext) throws Throwable {
		String taskid =jobContext.getJob().getParam("taskid");
		Map<String, Object> dataMap = new HashMap<String, Object>();
		dataMap.put("updatetime", System.currentTimeMillis());
		LOGGER.error("taskid="+taskid+",开始检查任务是否完成");
		if (UserManager.getInstance().checkTask(taskid)) {
			LOGGER.error("taskid="+taskid+",结束检查任务是否完成");
			dataMap.put("state", 1);
			if(LOGGER.isErrorEnabled()) {
				LOGGER.trace("taskid is finshed -------"+taskid+"-----"+JSON.toJSONString(dataMap) );
			}
			Job res=jobContext.getJob();
			res.setRepeatCount(0);
			res.setReplaceOnExist(true);
			jobContext.setJob(res);
		} else {
			LOGGER.error("taskid="+taskid+",结束检查任务是否完成");
			dataMap.put("state", 0);
		}
		dataMap.put("taskid", taskid);
		LOGGER.error("taskid="+taskid+"开始检查任务用户总数");
		dataMap.put("sumcount", UserManager.getInstance().getUserTaskCount(taskid, -1));
		LOGGER.error("taskid="+taskid+"结束检查任务用户总数");
		LOGGER.error("taskid="+taskid+"开始检查任务用户执行成功总数");
		dataMap.put("successcount", UserManager.getInstance().getUserTaskCount(taskid, 1));
		LOGGER.error("taskid="+taskid+"开始检查任务用户执行失败总数");
		dataMap.put("failcount", UserManager.getInstance().getUserTaskCount(taskid, 2));
		LOGGER.error("taskid="+taskid+"开始更新任务执行情况");
		UserManager.getInstance().updatetaskmsgliststate(dataMap);
		LOGGER.error("taskid="+taskid+"结束更新任务执行情况");
		return null;
	}
	
	private void TaskFinshe(String taskid) {
		Map<String, Object> dataMap = new HashMap<String, Object>();
		dataMap.put("updatetime", System.currentTimeMillis());
		LOGGER.error("taskid="+taskid+",开始检查任务是否完成");
		if (UserManager.getInstance().checkTask(taskid)) {
			LOGGER.error("taskid="+taskid+",结束检查任务是否完成");
			dataMap.put("state", 1);
			if(LOGGER.isErrorEnabled()) {
				LOGGER.trace("taskid is finshed -------"+taskid+"-----"+JSON.toJSONString(dataMap) );
			}
		} else {
			LOGGER.error("taskid="+taskid+",结束检查任务是否完成");
			dataMap.put("state", 0);
		}
		dataMap.put("taskid", taskid);
		LOGGER.error("taskid="+taskid+"开始检查任务用户总数");
		dataMap.put("sumcount", UserManager.getInstance().getUserTaskCount(taskid, -1));
		LOGGER.error("taskid="+taskid+"结束检查任务用户总数");
		LOGGER.error("taskid="+taskid+"开始检查任务用户执行成功总数");
		dataMap.put("successcount", UserManager.getInstance().getUserTaskCount(taskid, 1));
		LOGGER.error("taskid="+taskid+"开始检查任务用户执行失败总数");
		dataMap.put("failcount", UserManager.getInstance().getUserTaskCount(taskid, 2));
		LOGGER.error("taskid="+taskid+"开始更新任务执行情况");
		UserManager.getInstance().updatetaskmsgliststate(dataMap);
		LOGGER.error("taskid="+taskid+"结束更新任务执行情况");

	}

}
