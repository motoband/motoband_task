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
import com.google.gson.annotations.JsonAdapter;
import com.motoband.common.Consts;
import com.motoband.common.trace.TraceLevel;
import com.motoband.common.trace.Tracer;
import com.motoband.manager.ConfigManager;
import com.motoband.manager.DBConnectionManager;
import com.motoband.manager.DataVersionManager;
import com.motoband.manager.MotoDataManager;
import com.motoband.manager.UserManager;
import com.motoband.manager.YZManager;
import com.motoband.model.task.MBUserPushModel;
import com.motoband.model.task.MessageTaskModel;
import com.motoband.utils.OkHttpClientUtil;
import com.motoband.utils.RandomUtils;

public class TestJobRunnerTester extends JobRunnerTester {

    public static void main(String[] args) throws Throwable {
    	 System.setProperty("push_flag","0");
         System.setProperty("env","production");
         System.setProperty("env_task","production_task");
         System.setProperty("env_gps","production_gps");
     	DBConnectionManager.init("production");
     	DBConnectionManager.init("production_task");
     	DBConnectionManager.init("production_gps");
 		ConfigManager.getInstance().init("MotoBandTask");
 		MotoDataManager.getInstance().init();
 		DataVersionManager.getInstance().init();
 		DataVersionManager.getInstance().startCheck();	
 		OkHttpClientUtil.init();
 		YZManager.getInstance().refreshYZAccessToken();
//        //  Mock Job 数据
//    	String json="{\"createtime\":1586499906391,\"des\":\"不知这次有没有你，如没有，那是幸福\",\"failcount\":0,\"gpid\":0,\"handlecount\":0,\"id\":0,\"imgurl\":\"http://news2-10013836.cos.ap-shanghai.myqcloud.com/78217F5AC176441A95C30C86C6189E33\",\"linktype\":1,\"name\":\"2020-04-10 14:25:06_阿沟的推送\",\"nid\":\"FB7E8D79694A4038A46BC3FC90C4DC7A\",\"starttime\":0,\"state\":0,\"successcount\":0,\"sumcount\":0,\"taskid\":\"ios_push_20200410142501\",\"test\":1,\"title\":\"机车吐槽大会第二弹\",\"updatetime\":0,\"userpushmodel\":{\"addtime\":0,\"brandid\":0,\"brandparentid\":0,\"ctype\":1,\"cversion\":0,\"lastactivetime\":0,\"mbid\":0,\"modelid\":0,\"state\":0,\"updatetime\":0}}";
//    	MessageTaskModel taskModel=JSON.parseObject(json, MessageTaskModel.class);
//    	if(taskModel.userpushmodel==null) {
//    		taskModel.userpushmodel=new MBUserPushModel();
//    		//川
//    		List<String> userids=Lists.newArrayList("86-15910301209");
//    		//正威
//    		userids.add("417AA7D361914B38ABA6B59D74A9CE26");
////    		userids.add("E2CD901A398C4B66A4BB16CE1603B3B1");
////    		userids.add("E2CD901A398C4B66A4BB16CE1603B3B1");
//    		taskModel.userpushmodel.userids=userids;
//    	}else {
//    		List<String> userids=Lists.newArrayList("86-15910301209");
//    		//正威
//    		userids.add("417AA7D361914B38ABA6B59D74A9CE26");
//
//    		
//    		taskModel.userpushmodel.userids=userids;
//    	}
 		MessageTaskModel taskModel=new MessageTaskModel();
 		taskModel.taskid="cms_push_20200924184008";
 		taskModel.name="国庆约跑";
    	taskModel.title="国庆假期不知道去哪玩？不妨来这里看看 >>>";
    	taskModel.linktype=2;
    	taskModel.imgurl="https://motobox-10013836.image.myqcloud.com/B82480D9-2A6D-459A-9519-A5F62CD98666";
    	taskModel.keyword="约跑";
//    	taskModel.test=0;
//    	taskModel.subtitle=null;
//    	taskModel.des=null;
//    	taskModel.nid="1204E87D9F404AFF849EB6364A21E006";
    	//1586750375000
//    	taskModel.taskid+=System.currentTimeMillis()/1000;
    	taskModel.test=0;
    	Job job=new Job();
//    	job.setTaskId(MessageTaskModel.GPS_CHECKERRORRD);
//		job.setParam("type", MessageTaskModel.GPS_CHECKERRORRD);
//    	job.setTaskId(MessageTaskModel.GPS_PACKAGE);
//		job.setParam("type", MessageTaskModel.GPS_PACKAGE);
    	job.setTaskId(MessageTaskModel.YZ_REFRESH_ACCESSTOKEN);
		job.setParam("type", MessageTaskModel.YZ_REFRESH_ACCESSTOKEN);
		job.setTaskId(taskModel.taskid);
//		job.setParam("type", MessageTaskModel.PUSH_IM_PUSH);
//		job.setParam("data", JSON.toJSONString(taskModel));
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
//		job.setTriggerTime(taskModel.starttime);
//
//     	Job job=new Job();
////     	job.setParam("type", MessageTaskModel.SEARCH_SUGGEST);
////     	job.setParam("type", MessageTaskModel.NEWMOTOMODEL);
////     	job.setTaskId("test_newmotomodel_"+RandomUtils.randomNumber(0, 999999));
//     	job.setParam("type", MessageTaskModel.NEWMOTOMODEL_RANK);
//     	job.setTaskId("test_newmotomodel_rank"+RandomUtils.randomNumber(0, 999999));
//     	job.setParam("year","2020");
//     	job.setParam("month","6");
//     	System.out.println(JSON.toJSONString(job.getExtParams()));
    	JobContext jobContext = new JobContext();
        jobContext.setJob(job);

        JobExtInfo jobExtInfo = new JobExtInfo();
        jobExtInfo.setRetry(false);
//
        jobContext.setJobExtInfo(jobExtInfo);
       
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
//		UserManager.getInstance().addMessageTask(taskModel);
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
