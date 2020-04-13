package com.motobang.task;

import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.List;

import org.apache.commons.lang3.StringUtils;

import com.alibaba.fastjson.JSON;
import com.github.ltsopensource.core.domain.Job;
import com.github.ltsopensource.tasktracker.Result;
import com.github.ltsopensource.tasktracker.runner.JobContext;
import com.github.ltsopensource.tasktracker.runner.JobExtInfo;
import com.github.ltsopensource.tasktracker.runner.JobRunner;
import com.github.ltsopensource.tasktracker.runner.JobRunnerTester;
import com.google.common.collect.Lists;
import com.motoband.common.Consts;
import com.motoband.common.trace.TraceLevel;
import com.motoband.common.trace.Tracer;
import com.motoband.manager.ConfigManager;
import com.motoband.manager.DBConnectionManager;
import com.motoband.manager.DataVersionManager;
import com.motoband.manager.MotoDataManager;
import com.motoband.manager.UserManager;
import com.motoband.model.task.MBUserPushModel;
import com.motoband.model.task.MessageTaskModel;
import com.motoband.utils.OkHttpClientUtil;

public class TestJobRunnerTester extends JobRunnerTester {

    public static void main(String[] args) throws Throwable {
//        //  Mock Job 数据
    	String json="{\"createtime\":1586499906391,\"des\":\"不知这次有没有你，如没有，那是幸福\",\"failcount\":0,\"gpid\":0,\"handlecount\":0,\"id\":0,\"imgurl\":\"http://news2-10013836.cos.ap-shanghai.myqcloud.com/78217F5AC176441A95C30C86C6189E33\",\"linktype\":1,\"name\":\"2020-04-10 14:25:06_阿沟的推送\",\"nid\":\"FB7E8D79694A4038A46BC3FC90C4DC7A\",\"starttime\":0,\"state\":0,\"successcount\":0,\"sumcount\":0,\"taskid\":\"ios_push_20200410142506\",\"test\":1,\"title\":\"机车吐槽大会第二弹\",\"updatetime\":0,\"userpushmodel\":{\"addtime\":0,\"brandid\":0,\"brandparentid\":0,\"ctype\":1,\"cversion\":0,\"lastactivetime\":0,\"mbid\":0,\"modelid\":0,\"state\":0,\"updatetime\":0}}";
    	MessageTaskModel taskModel=JSON.parseObject(json, MessageTaskModel.class);
    	if(taskModel.userpushmodel==null) {
    		taskModel.userpushmodel=new MBUserPushModel();
    		//川
    		List<String> userids=Lists.newArrayList("1C90B36CAA8D4B4EAF59A866CA7170E9");
    		//正威
    		userids.add("8CA00FA094C14FBC88FC8ECFF92152A0");
    		userids.add("E2CD901A398C4B66A4BB16CE1603B3B1");
    		
    		taskModel.userpushmodel.userids=userids;
    	}else {
    		List<String> userids=Lists.newArrayList("1C90B36CAA8D4B4EAF59A866CA7170E9");
    		//正威
    		userids.add("8CA00FA094C14FBC88FC8ECFF92152A0");
    		
    		taskModel.userpushmodel.userids=userids;
    	}
    	//1586750375000
    	taskModel.taskid+=System.currentTimeMillis()/1000000000;
    	taskModel.test=1;
    	Job job=new Job();
		job.setTaskId(taskModel.taskid);
		job.setParam("type", MessageTaskModel.PUSH_IM_PUSH);
		job.setParam("data", JSON.toJSONString(taskModel));
		job.setNeedFeedback(true);
		job.setRelyOnPrevCycle(true);
		job.setReplaceOnExist(true);
//		job.setTriggerTime(taskModel.starttime);
		if(StringUtils.isNotBlank(taskModel.cron)) {
			job.setCronExpression(taskModel.cron);
		}
		job.setTaskTrackerNodeGroup(Consts.LTS_NODE_GROUP);
//		if(taskModel.starttime==0&&StringUtils.isNotBlank(taskModel.cron)) {
//			return MBResponse.getMBResponse(MBResponseCode.ERROR);
//		}
		long time=System.currentTimeMillis();
		if(taskModel.starttime!=0) {
			time=taskModel.starttime;
		}
		if(taskModel.test==0) {
			taskModel.starttime=LocalDateTime.ofEpochSecond(time/1000, 0, ZoneOffset.of("+8")).plusMinutes(0).toInstant(ZoneOffset.of("+8")).toEpochMilli();

		}else if (taskModel.test==1) {
			taskModel.starttime=LocalDateTime.ofEpochSecond(time/1000, 0, ZoneOffset.of("+8")).plusMinutes(5).toInstant(ZoneOffset.of("+8")).toEpochMilli();

		}
		UserManager.getInstance().addMessageTask(taskModel);
		job.setTriggerTime(taskModel.starttime);
//
        JobContext jobContext = new JobContext();
        jobContext.setJob(job);

        JobExtInfo jobExtInfo = new JobExtInfo();
        jobExtInfo.setRetry(false);
//
        jobContext.setJobExtInfo(jobExtInfo);
        System.setProperty("push_flag","0");
    	DBConnectionManager.init("production");
		ConfigManager.getInstance().init("MotoBandTask");
		MotoDataManager.getInstance().init();
		DataVersionManager.getInstance().init();
		DataVersionManager.getInstance().startCheck();	
		OkHttpClientUtil.init();
		String cfgPath=null;
		if(args.length!=0) {
			cfgPath = args[0];
		}
//        start(cfgPath);
        TraceLevel tiTraceLevel = TraceLevel.get(Integer.valueOf(ConfigManager.getInstance().getConfig(ConfigManager.Trace_Level)));
		boolean writeToFile = Boolean.valueOf(ConfigManager.getInstance().getConfig(ConfigManager.Trace_WriteTraceToFile));
		boolean printToControl = Boolean.valueOf(ConfigManager.getInstance().getConfig(ConfigManager.Trace_PrintTraceToControl));
		String logFileURL = ConfigManager.getInstance().getConfig(ConfigManager.Trace_LogFileURL);
		Tracer.initialize( tiTraceLevel, writeToFile, printToControl, ConfigManager.ServiceName, "/data/logs/");
//		if (_tracer.CriticalAvailable())
//			_tracer.Critical(TaskTrackerStartup.class.getSimpleName()+" MotoBandTask init  SUCCESSFUL");
//		LOGGER.info(TaskTrackerStartup.class.getSimpleName()+" MotoBandTask init  SUCCESSFUL");

        // 运行测试
        TestJobRunnerTester tester = new TestJobRunnerTester();
        Result result = tester.run(jobContext);
        System.out.println(JSON.toJSONString(result));
    }

    @Override
    protected void initContext() {
        // TODO 初始化Spring容器
    }

    @Override
    protected JobRunner newJobRunner() {
        return new JobRunnerDispatcher();
    }
}