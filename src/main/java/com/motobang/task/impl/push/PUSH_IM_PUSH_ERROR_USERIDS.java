package com.motobang.task.impl.push;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.alibaba.fastjson.JSON;
import com.github.ltsopensource.core.domain.Action;
import com.github.ltsopensource.core.logger.Logger;
import com.github.ltsopensource.core.logger.LoggerFactory;
import com.github.ltsopensource.tasktracker.Result;
import com.github.ltsopensource.tasktracker.runner.InterruptibleJobRunner;
import com.github.ltsopensource.tasktracker.runner.JobContext;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.motoband.common.Consts;
import com.motoband.dao.lts.LTSDAO;
import com.motoband.manager.RedisManager;
import com.motoband.manager.UserManager;
import com.motoband.utils.OkHttpClientUtil;
import com.motoband.utils.collection.CollectionUtil;

import okhttp3.Headers;

public class PUSH_IM_PUSH_ERROR_USERIDS implements InterruptibleJobRunner {
    protected static final Logger LOGGER = LoggerFactory.getLogger(PUSH_IM_PUSH_ERROR_USERIDS.class);
public static void main(String[] args) {
	String[] taskname="server_push_1584675965".split("_");
	String time=taskname[taskname.length-1];
	taskname=Arrays.copyOfRange(taskname, 0, taskname.length-1);
	String newTaskId=CollectionUtil.join(Lists.newArrayList(taskname), "_")+"_callback_"+time;
	System.out.println(newTaskId);
}
	@Override
	public Result run(JobContext jobContext) throws Throwable {
		String taskid=jobContext.getJob().getParam("taskid");
		List<String> userids = JSON.parseArray(jobContext.getJob().getParam("userids"),String.class);
		List<String> erroruserids = JSON.parseArray(jobContext.getJob().getParam("erroruserids"),String.class);

		LOGGER.error("taskid="+jobContext.getJob().getTaskId()+",线程id="+Thread.currentThread().getId()+",执行推送完毕,开始更改数据库用户状态");
		if (erroruserids != null && erroruserids.size() > 0) {
			Map<String, Object> dataMap = new HashMap<String, Object>();
			userids.removeAll(erroruserids);
			dataMap.put("userids", userids);
			dataMap.put("state", 1);
			dataMap.put("taskid", taskid);
			// 更新用户任务完成情况
			UserManager.getInstance().updateUsertaskmsg(dataMap);
			dataMap.clear();
			if (LOGGER.isErrorEnabled()) {
				LOGGER.error("任务id" + taskid + "-失败用户:" + JSON.toJSONString(erroruserids));
			}
			// 失败任务
			dataMap.put("userids", erroruserids);
			dataMap.put("state", 2);
			dataMap.put("taskid", taskid);
			UserManager.getInstance().updateUsertaskmsg(dataMap);
			LOGGER.error("taskid="+taskid+",线程id="+Thread.currentThread().getId()+",执行推送完毕,结束更改数据库用户状态");
		} else {
					Map<String, Object> dataMap = new HashMap<String, Object>();
					dataMap.put("userids", userids);
					dataMap.put("state", 1);
					dataMap.put("taskid", taskid);
					UserManager.getInstance().updateUsertaskmsg(dataMap);
					LOGGER.error("taskid="+taskid+",线程id="+Thread.currentThread().getId()+",执行推送完毕,人數:"+userids.size()+"结束更改数据库用户状态");
				}
//		Map<String,Object> map=LTSDAO.getLTSTaskRepeat(jobContext.getJob().getTaskId());
//		if(map!=null) {
//			String job_id=(String) map.get("job_id");
//			Map<String,String> params=Maps.newHashMap();
//			params.put("jobId", job_id);
//			okhttp3.Response response=null;
//			try {
//				Map<String,String> r=Maps.newHashMap();
//				r.put("Authorization", "Basic bW90b2JhbmQ6TW90b2JhbmQyMDE1IUAjJA==");
//				Headers.of(r);
//				response=OkHttpClientUtil.okHttpPost(Consts.LTS_ADMIN_API_IP+"/api/job-queue/repeat-job-delete",params,Headers.of(r));
//				RedisManager.getInstance().delbykey(Consts.REDIS_SCHEME_RUN, jobContext.getJob().getTaskId());
//				return null;
//			} catch (Exception e) {
//				throw e;
//			}finally {
//				if(response!=null) {
//					response.close();
//				}
//				
//			}
//		}
		//要回调2600次,此处不处理任务是否完成
//		TaskFinshe(taskid);
		
		return new Result(Action.EXECUTE_SUCCESS);
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
