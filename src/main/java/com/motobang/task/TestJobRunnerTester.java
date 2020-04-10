package com.motobang.task;

import java.time.LocalDateTime;
import java.time.ZoneOffset;

import org.apache.commons.lang3.StringUtils;

import com.alibaba.fastjson.JSON;
import com.github.ltsopensource.core.domain.Job;
import com.github.ltsopensource.tasktracker.Result;
import com.github.ltsopensource.tasktracker.runner.JobContext;
import com.github.ltsopensource.tasktracker.runner.JobExtInfo;
import com.github.ltsopensource.tasktracker.runner.JobRunner;
import com.github.ltsopensource.tasktracker.runner.JobRunnerTester;
import com.motoband.common.Consts;
import com.motoband.common.trace.TraceLevel;
import com.motoband.common.trace.Tracer;
import com.motoband.manager.ConfigManager;
import com.motoband.manager.DBConnectionManager;
import com.motoband.manager.DataVersionManager;
import com.motoband.manager.MotoDataManager;
import com.motoband.model.task.MessageTaskModel;
import com.motoband.utils.OkHttpClientUtil;

public class TestJobRunnerTester extends JobRunnerTester {

    public static void main(String[] args) throws Throwable {
//        //  Mock Job 数据
    	String json=""
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
		job.setTriggerTime(taskModel.starttime);
//
        JobContext jobContext = new JobContext();
        jobContext.setJob(job);

        JobExtInfo jobExtInfo = new JobExtInfo();
        jobExtInfo.setRetry(false);
//
        jobContext.setJobExtInfo(jobExtInfo);
    	DBConnectionManager.init();
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