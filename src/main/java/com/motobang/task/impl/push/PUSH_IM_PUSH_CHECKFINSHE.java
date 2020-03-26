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
import com.motoband.manager.UserManager;
import com.motoband.model.task.MessageTaskModel;
import com.motoband.utils.OkHttpClientUtil;

import okhttp3.Headers;

public class PUSH_IM_PUSH_CHECKFINSHE implements InterruptibleJobRunner {
    protected static final Logger LOGGER = LoggerFactory.getLogger(PUSH_IM_PUSH_CHECKFINSHE.class);

	@Override
	public Result run(JobContext jobContext) throws Throwable {
		String taskid =jobContext.getJob().getParam("taskid");
		Map<String, Object> dataMap = new HashMap<String, Object>();
		MessageTaskModel taskModel=UserDAO.getTaskMsgByTaskid(taskid);
		if(taskModel.state==3) {
			return null;
		}
		dataMap.put("updatetime", System.currentTimeMillis());
		if (UserManager.getInstance().checkTask(taskid)) {
			dataMap.put("state", 1);
			if(LOGGER.isErrorEnabled()) {
				LOGGER.trace("taskid is finshed -------"+taskid+"-----"+JSON.toJSONString(dataMap) );
			}
			Map<String,Object> map=UserDAO.getLTSTask(taskid);
			if(map!=null) {
				String job_id=(String) map.get("job_id");
				Map<String,String> params=Maps.newHashMap();
				params.put("jobId", job_id);
				okhttp3.Response response=null;
				try {
					Map<String,String> r=Maps.newHashMap();
					r.put("Authorization", "Basic bW90b2JhbmQ6TW90b2JhbmQyMDE1IUAjJA==");
					Headers.of(r);
					response=OkHttpClientUtil.okHttpPost(Consts.LTS_ADMIN_API_IP+"/api/job-queue/repeat-job-delete",params,Headers.of(r));
					if(response.isSuccessful()){
					}
				} catch (Exception e) {
					e.printStackTrace();
				}finally {
					if(response!=null) {
						response.close();
					}
				}
			}
		} else {
			dataMap.put("state", 0);
		}
		dataMap.put("taskid", taskid);
		dataMap.put("sumcount", UserManager.getInstance().getUserTaskCount(taskid, -1));
		dataMap.put("successcount", UserManager.getInstance().getUserTaskCount(taskid, 1));
		dataMap.put("failcount", UserManager.getInstance().getUserTaskCount(taskid, 2));
		UserManager.getInstance().updatetaskmsgliststate(dataMap);
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

	@Override
	public void interrupt() {
		LOGGER.info("结束处理...............");		
	}

}
