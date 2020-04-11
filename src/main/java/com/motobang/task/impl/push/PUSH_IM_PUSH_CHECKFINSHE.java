package com.motobang.task.impl.push;

import java.util.HashMap;
import java.util.Map;

import com.alibaba.fastjson.JSON;
import com.github.ltsopensource.core.domain.Job;
import com.github.ltsopensource.core.logger.Logger;
import com.github.ltsopensource.core.logger.LoggerFactory;
import com.github.ltsopensource.tasktracker.Result;
import com.github.ltsopensource.tasktracker.runner.InterruptibleJobRunner;
import com.github.ltsopensource.tasktracker.runner.JobContext;
import com.google.common.collect.Maps;
import com.motoband.common.Consts;
import com.motoband.common.MBResponse;
import com.motoband.common.MBResponseCode;
import com.motoband.dao.UserDAO;
import com.motoband.dao.lts.LTSDAO;
import com.motoband.manager.RedisManager;
import com.motoband.manager.UserManager;
import com.motoband.model.task.MessageTaskModel;
import com.motoband.utils.OkHttpClientUtil;

import okhttp3.Headers;

public class PUSH_IM_PUSH_CHECKFINSHE implements InterruptibleJobRunner {
    protected static final Logger LOGGER = LoggerFactory.getLogger(PUSH_IM_PUSH_CHECKFINSHE.class);

	@Override
	public Result run(JobContext jobContext) throws Throwable {
		//任务是否被中断
		//检测回调任务是否完成
		//检测任务数是否一致
		String taskid =jobContext.getJob().getParam("taskid");
		if(checkTaskInterupte(taskid)) {
			cancleTask(jobContext, taskid);
			return null;
		}
		if(!checkCallbackStatus(taskid)) {
			return null;
		}
		Map<String, Object> dataMap = new HashMap<String, Object>();
		MessageTaskModel taskModel=UserDAO.getTaskMsgByTaskid(taskid);
		boolean flag=false;
		if(taskModel==null){
			flag=true;
		}
		if(taskModel!=null&&taskModel.state==3) {
			flag=true;
		}
		dataMap.put("updatetime", System.currentTimeMillis());
		if (UserManager.getInstance().checkTask(taskid)) {
			dataMap.put("state", 1);
			if(LOGGER.isErrorEnabled()) {
				LOGGER.trace("taskid is finshed -------"+taskid+"-----"+JSON.toJSONString(dataMap) );
			}
			flag=true;
		} else {
			dataMap.put("state", 0);
		}
		if(flag) {
//			dataMap.put("taskid", taskid);
//			dataMap.put("sumcount", UserManager.getInstance().getUserTaskCount(taskid, -1));
//			dataMap.put("successcount", UserManager.getInstance().getUserTaskCount(taskid, 1));
//			dataMap.put("failcount", UserManager.getInstance().getUserTaskCount(taskid, 2));
//			UserManager.getInstance().updatetaskmsgliststate(dataMap);
			cancleTask(jobContext, taskid);
		}
		dataMap.put("taskid", taskid);
		dataMap.put("sumcount", UserManager.getInstance().getUserTaskCount(taskid, -1));
		dataMap.put("successcount", UserManager.getInstance().getUserTaskCount(taskid, 1));
		dataMap.put("failcount", UserManager.getInstance().getUserTaskCount(taskid, 2));
		UserManager.getInstance().updatetaskmsgliststate(dataMap);
		return null;
	}
	/**
	 * 检测任务回调是否已完成
	 * @param taskids
	 * @return
	 */
	private boolean checkCallbackStatus(String taskids) {
		return LTSDAO.getLTSCallBack(taskids);
	}

	private void cancleTask(JobContext jobContext, String taskid) throws Exception {
		Map<String, Object> map = LTSDAO.getLTSTaskRepeat(jobContext.getJob().getTaskId());
		if (map != null) {
			String job_id = (String) map.get("job_id");
			Map<String, String> params = Maps.newHashMap();
			params.put("jobId", job_id);
			okhttp3.Response response = null;
			try {
				Map<String, String> r = Maps.newHashMap();
				r.put("Authorization", "Basic bW90b2JhbmQ6TW90b2JhbmQyMDE1IUAjJA==");
				Headers.of(r);
				response = OkHttpClientUtil.okHttpPost(Consts.LTS_ADMIN_API_IP + "/api/job-queue/repeat-job-delete",
						params, Headers.of(r));
				RedisManager.getInstance().delbykey(Consts.REDIS_SCHEME_RUN, taskid);
			} catch (Exception e) {
				throw e;
			} finally {
				if (response != null) {
					response.close();
				}

			}
		}
	}
	
	/**
	 * 检测任务是否被中断
	 * @param taskid
	 * @return
	 */
	private boolean checkTaskInterupte(String taskid) {
		Map<String,Object> map=LTSDAO.getLTSTaskExecuting(taskid);
		if(map!=null) {
			return false;
		}
		return true;
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

	@Override
	public void interrupt() {
		LOGGER.info("结束处理...............");		
	}

}
