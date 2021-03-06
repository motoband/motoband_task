package com.motobang.task.impl.push;

import java.time.Clock;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.exception.ExceptionUtils;

import com.alibaba.fastjson.JSON;
import com.github.ltsopensource.core.domain.Action;
import com.github.ltsopensource.core.logger.Logger;
import com.github.ltsopensource.core.logger.LoggerFactory;
import com.github.ltsopensource.tasktracker.Result;
import com.github.ltsopensource.tasktracker.runner.InterruptibleJobRunner;
import com.github.ltsopensource.tasktracker.runner.JobContext;
import com.google.common.collect.Maps;
import com.motoband.common.Consts;
import com.motoband.dao.UserDAO;
import com.motoband.manager.MBMessageManager;
import com.motoband.manager.MotoDataManager;
import com.motoband.manager.RedisManager;
import com.motoband.manager.UserManager;
import com.motoband.model.BannerModel;
import com.motoband.model.MBMessageModel;
import com.motoband.model.SimpleUserModel;
import com.motoband.model.task.MessageTaskModel;
import com.motoband.utils.ExecutorsUtils;
import com.motoband.utils.collection.CollectionUtil;

/**
 * 推送任务
 * Created by junfei.Yang on 2020年3月12日.
 */
public class PUSH_IM_PUSH implements InterruptibleJobRunner {
    protected static final Logger LOGGER = LoggerFactory.getLogger(PUSH_IM_PUSH.class);
	@Override
	public Result run(JobContext jobContext) throws Throwable {
		LOGGER.info("开始处理任务 jobContext="+JSON.toJSONString(jobContext));
		String taskid=jobContext.getJob().getTaskId();
		String data=jobContext.getJob().getParam("data");
//		 BizLOGGER bizLOGGER = LtsLOGGERFactory.getBizLOGGER();
		try {
            // TODO 业务逻辑
            // 会发送到 LTS (JobTracker上)
//            bizLOGGER.info("测试，业务日志啊啊啊啊啊");
//            bizLOGGER.info("jobContext="+JSON.toJSONString(jobContext));
			if(StringUtils.isNotBlank(data)) {
				MessageTaskModel taskModel=JSON.parseObject(data, MessageTaskModel.class);
//				UserManager.getInstance().addMessageTask(taskModel);
				LOGGER.error("taskid="+taskid+",开始插入需要推送的用户");
//				if(taskModel.test==1) {
//					StringBuffer sql=new StringBuffer("select * from mbuser_push where 1=1");
//					if(taskModel!=null) {
//						if(taskModel.userpushmodel.addtime>0) {
//							sql.append("addtime>="+taskModel.userpushmodel.addtime);
//						}
//					}
//				}
				UserManager.getInstance().addMessageTaskUserAll(taskModel);
				Map<String, Object> dataMap = new HashMap<String, Object>();
				dataMap.put("taskid", taskModel.taskid);
				dataMap.put("updatetime", System.currentTimeMillis());
				dataMap.put("state", 2);
				UserManager.getInstance().updatetaskmsgliststate(dataMap);
				LOGGER.error("taskid="+taskid+",开始查询推送的MBMessageModel");
				MBMessageModel model = gettaskMessageModel(taskModel);
				LOGGER.error("taskid="+taskid+",结束查询推送的MBMessageModel="+JSON.toJSONString(model));
				String pushMsg = "您有一条新的消息，点击查看";
				if(model.bannermodel!=null){
					pushMsg =model.bannermodel.title;
				}
				//标记需要处理的用户 条件是有效的用户  需要先筛选出有效的用户 条件是半年以内登录过的用户
				FenPiSendtaskMsg_new(taskid, model, pushMsg, 0);
				dataMap.put("successcount", UserManager.getInstance().getUserTaskCount(taskid, 0));
				dataMap.put("sumscount", UserManager.getInstance().getUserTaskCount(taskid, 0));
//				UserManager.getInstance().updatetaskmsgliststate(dataMap);
//				dataMap = new HashMap<String, Object>();
//				dataMap.put("taskid", taskModel.taskid);
				dataMap.put("updatetime", System.currentTimeMillis());
				dataMap.put("state", 1);
				UserManager.getInstance().updatetaskmsgliststate(dataMap);
			}
        } catch (Exception e) {
        	if(e.getMessage().contains("interrupt")) {
        		Map<String, Object> dataMap = new HashMap<String, Object>();
				dataMap.put("taskid", taskid);
				dataMap.put("updatetime", System.currentTimeMillis());
				dataMap.put("state", 3);
				UserManager.getInstance().updatetaskmsgliststate(dataMap);
        		return null;
        	}else {
        		LOGGER.error("ERROR="+ExceptionUtils.getStackTrace(e));
                return new Result(Action.EXECUTE_FAILED, ExceptionUtils.getStackTrace(e));
        	}
        }
		Map<String,Integer> m=Maps.newHashMap();
		if(MotoDataManager.getInstance().PUSH_IM_PUSH_MAP.containsKey(taskid)) {
			m.put(taskid, MotoDataManager.getInstance().PUSH_IM_PUSH_MAP.get(taskid).intValue());
			LOGGER.error(
					String.format("taskid:%s,共发出:%s 个用户消息", taskid, MotoDataManager.getInstance().PUSH_IM_PUSH_MAP.get(taskid)));
			MotoDataManager.getInstance().PUSH_IM_PUSH_MAP.remove(taskid);
		}
        return new Result(Action.EXECUTE_SUCCESS, JSON.toJSONString(m));
	}
	
	private void FenPiSendtaskMsg_new(String taskid, MBMessageModel model, String pushMsg, int pici) throws Exception {
		Map<String, Object> dataMap = new HashMap<String, Object>();
		dataMap.put("taskid", taskid);
		dataMap.put("pici", pici*80000);
		Clock c=Clock.systemUTC();
		long time=c.millis();
		LOGGER.error(
				String.format("taskid:%s,开始查询第 pici:%s 批次用户", taskid, pici));
		List<String> userids = UserManager.getInstance().getMessageTaskUserByTaskid(dataMap);
		if (LOGGER.isErrorEnabled()) {
			LOGGER.error(
					String.format("taskid:%s 结束查询第 pici:%s 批次用户,用时:%s,userids:%s", taskid, pici,c.millis()-time, userids==null?0:userids.size()));
		}
		if(userids==null||userids.size()==0) {
			TaskFinshe(taskid);
			return ;
		}
		LOGGER.error("taskid="+taskid+",开始推送");
		pici = batchSendCMSMessage(taskid, model, pushMsg, pici, userids);
		LOGGER.error("taskid="+taskid+",结束推送");
		FenPiSendtaskMsg_new(taskid, model, pushMsg, pici);		
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
		dataMap.put("sumcount", UserManager.getInstance().getUserTaskCount(taskid, 0));
		LOGGER.error("taskid="+taskid+"结束检查任务用户总数");
		LOGGER.error("taskid="+taskid+"开始检查任务用户执行成功总数");
		dataMap.put("successcount", UserManager.getInstance().getUserTaskCount(taskid, 0));
		LOGGER.error("taskid="+taskid+"开始检查任务用户执行失败总数");
		dataMap.put("failcount", UserManager.getInstance().getUserTaskCount(taskid, 2));
		LOGGER.error("taskid="+taskid+"开始更新任务执行情况");
		UserManager.getInstance().updatetaskmsgliststate(dataMap);
		LOGGER.error("taskid="+taskid+"结束更新任务执行情况");

	}

	private int batchSendCMSMessage(String taskid, MBMessageModel model, String pushMsg, int pici, List<String> userids)throws Exception {
		if (userids != null && userids.size() > 0) {
			int c=Runtime.getRuntime().availableProcessors()*5;
			if(c>=10){
				c=10;
			}
//			c=1;
			LOGGER.error("taskid="+taskid+",开始多线程执行推送任务,多线程数量"+c);
			RedisManager.getInstance().delbykey(Consts.REDIS_SCHEME_RUN, model.taskid);
			List<List<String>> res = CollectionUtil.averageAssign(userids, c);
//			List<List<String>> res = CollectionUtil.averageAssign(userids, 50);
			CyclicBarrier cb = new CyclicBarrier(res.size() + 1);
			AtomicInteger groupcountAtomic = new AtomicInteger(0);
			for (List<String> innerlist : res) {
				final int thread_pici = pici;
				ExecutorsUtils.getInstance().submit(new Runnable() {
					@Override
					public void run() {
						LOGGER.error("taskid="+taskid+Thread.currentThread().getId()+"innerlist="+innerlist.size());
						if(CollectionUtil.isEmpty(innerlist)) {
							try {
								cb.await();
							} catch (InterruptedException | BrokenBarrierException e) {
								e.printStackTrace();
							}
						}
//						LOGGER.error("taskid="+taskid+Thread.currentThread().getId()+"开始推送"+"innerlist="+JSON.toJSONString(innerlist));
						int groupcount = groupcountAtomic.incrementAndGet();
						double forcountdouble = Math.ceil(innerlist.size() / 400.0);
						int forcount = (int) forcountdouble;
						List<List<String>> msglist = CollectionUtil.averageAssign(innerlist, forcount);
						int classcount = 1;
						for (List<String> sendlist : msglist) {
							LOGGER.error("taskid="+taskid+Thread.currentThread().getId()+"开始推送"+"sendlist="+sendlist.size());
							if (LOGGER.isErrorEnabled()) {
//								LOGGER.error(String.format("taskid:%s,pici:%s,groupcount:%s,classcount:%s", taskid,
//										thread_pici, groupcount, classcount));
//								LOGGER.error("taskid="+taskid+",线程id="+Thread.currentThread().getId()+",执行用户数量="+innerlist.size()+",分组数量="+forcount
//										+",开始执行第"+classcount+"分组");

							}
							model.taskreq=model.taskid+"_callback_"+RedisManager.getInstance().string_incr(Consts.REDIS_SCHEME_RUN, model.taskid);
							Integer size=sendlist.size();
							classcount++;
							LOGGER.error("taskid="+taskid+",taskreq="+model.taskreq+"开始插入缓存MAP");
							if(MotoDataManager.getInstance().PUSH_IM_PUSH_MAP.containsKey(model.taskid)) {
//								size.set(timMessage.To_Account.size());
								MotoDataManager.getInstance().PUSH_IM_PUSH_MAP.get(model.taskid).addAndGet(sendlist.size());
							}else {
								MotoDataManager.getInstance().PUSH_IM_PUSH_MAP.put(model.taskid, new AtomicInteger(sendlist.size()));
							}
//							if(MotoDataManager.getInstance().PUSH_IM_PUSH_MAP.containsKey(taskid)) {
//								 size=sendlist.size()+MotoDataManager.getInstance().PUSH_IM_PUSH_MAP.get(taskid);
//							}else {
//								 size=sendlist.size();
//							}
//							MotoDataManager.getInstance().PUSH_IM_PUSH_MAP.put(taskid, size);
							LOGGER.error("taskid="+taskid+",taskreq="+model.taskreq+"结束插入缓存MAP="+size);
							LOGGER.error("taskid="+taskid+",taskreq="+model.taskreq+",开始执行执行用户数量="+sendlist.size()+",分组数量="+forcount+",开始执行第"+classcount+"分组");
							singleSendtaskMsg(taskid, model, pushMsg, sendlist);
							LOGGER.error("taskid="+taskid+",taskreq="+model.taskreq+",结束执行执行用户数量="+sendlist.size()+",分组数量="+forcount+",结束执行第"+classcount+"分组");

							if (LOGGER.isErrorEnabled()) {
//								LOGGER.error(String.format("taskid:%s,pici:%s,groupcount:%s,classcount:%s", taskid,
//										thread_pici, groupcount, classcount));
//								LOGGER.error("taskid="+taskid+",线程id="+Thread.currentThread().getId()+",执行用户数量="+innerlist.size()+",分组数量="+forcount
//										+",结束执行第"+classcount+"分组");
							}
							// System.out.print("====400===");
							// System.out.println("====400===");
						}

						try {
//							executor.submit(new Runnable() {
//							@Override
//							public void run() {
								
//							}
//						});
							cb.await();
						} catch (InterruptedException | BrokenBarrierException e) {
							if (LOGGER.isErrorEnabled()) {
								LOGGER.error(ExceptionUtils.getStackTrace(e));
								
							}
							try {
								throw e;
							} catch (Exception e1) {
								e1.printStackTrace();
							}
						}

					}

				});
			}
			try {
				cb.await();
				pici++;
				if (LOGGER.isErrorEnabled()) {
					LOGGER.error(String.format("taskid:%s,pici:%s is over,start next", taskid, pici - 1));
				}

			} catch (InterruptedException e) {
				if (LOGGER.isErrorEnabled()) {
					LOGGER.error(ExceptionUtils.getStackTrace(e));
				}
			} catch (BrokenBarrierException e) {
				if (LOGGER.isErrorEnabled()) {
					LOGGER.error(ExceptionUtils.getStackTrace(e));
				}
			}
		}
		return pici;
	}

	protected void singleSendtaskMsg(String taskid, MBMessageModel model, String pushMsg, List<String> sendlist) {
		model.taskid=taskid;
		MBMessageManager.getInstance().sendMessage(model, sendlist, pushMsg,false);
	}

	public MBMessageModel gettaskMessageModel(MessageTaskModel messageTaskModel) {

		MBMessageModel model = new MBMessageModel();
		model.taskid=messageTaskModel.taskid;
		BannerModel bannermodel = new BannerModel();
		if (messageTaskModel != null) {
			bannermodel.title=messageTaskModel.title;
			bannermodel.subtitle=messageTaskModel.subtitle;
			bannermodel.des=messageTaskModel.des;
			bannermodel.linktype=messageTaskModel.linktype;
			bannermodel.linkurl=messageTaskModel.linkurl;
			bannermodel.imgurl=messageTaskModel.imgurl;
			bannermodel.gpid=messageTaskModel.gpid;
			bannermodel.nid=messageTaskModel.nid;
			if(StringUtils.isNotBlank(bannermodel.nid)) {
				String type=RedisManager.getInstance().hget(Consts.REDIS_SCHEME_NEWS, messageTaskModel.nid+"_ninfo", "type");
				if(StringUtils.isNotBlank(type)) {
					bannermodel.ntype=Integer.parseInt(type);
				}else {
					bannermodel.ntype=0;
				}
				model.nid=messageTaskModel.nid;
				model.ntype=bannermodel.ntype;
			}
			
			bannermodel.keyword=messageTaskModel.keyword;
			bannermodel.secondcarid=messageTaskModel.secondcarid;
			bannermodel.miniprogramid=messageTaskModel.miniprogramid;
			bannermodel.buserid=messageTaskModel.buserid;
			bannermodel.groupid=messageTaskModel.groupid;
		}
		model.bannermodel=bannermodel;
		model.msgtype=MBMessageModel.MBMsgType_Banner;
		model.msgtime=System.currentTimeMillis();
		model.content=messageTaskModel.title;
		
		SimpleUserModel simpleUserInfo = UserManager.getInstance().getSimpleUserInfo(Consts.TIM_ACTIVITYCENTERID);
		if (simpleUserInfo != null) {
			model.simpleusermodel=simpleUserInfo;
		}
		return model;

	}

	@Override
	public void interrupt() {
		LOGGER.info("结束处理...............");
	}
}