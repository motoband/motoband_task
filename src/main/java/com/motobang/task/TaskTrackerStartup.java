package com.motobang.task;

import org.apache.log4j.Level;

import com.github.ltsopensource.core.logger.Logger;
import com.github.ltsopensource.core.logger.LoggerFactory;
import com.github.ltsopensource.tasktracker.TaskTracker;
import com.motoband.common.Consts;
import com.motoband.common.trace.TraceLevel;
import com.motoband.common.trace.Tracer;
import com.motoband.manager.ConfigManager;
import com.motoband.manager.DBConnectionManager;
import com.motoband.manager.DataVersionManager;
import com.motoband.manager.MotoDataManager;
import com.motoband.manager.YZManager;
import com.motoband.utils.OkHttpClientUtil;
import com.qcloud.cos.COSClient;
import com.qcloud.cos.ClientConfig;
import com.qcloud.cos.auth.BasicCOSCredentials;
import com.qcloud.cos.auth.COSCredentials;
import com.qcloud.cos.region.Region;

/**
 * 
 * Created by junfei.Yang on 2020年3月11日.
 */
public class TaskTrackerStartup {
    protected static final Logger LOGGER = LoggerFactory.getLogger(TaskTrackerStartup.class);
	private static final Tracer _tracer = Tracer.create(TaskTrackerStartup.class);

    public static void main(String[] args) throws Exception {
//    	System.setProperty("env_task","production_task");
    	DBConnectionManager.init();
    	DBConnectionManager.init(Consts.DB_ENVIRONMENT_TASK);
    	DBConnectionManager.init(Consts.DB_ENVIRONMENT_GPS);
		ConfigManager.getInstance().init("MotoBandTask");
		MotoDataManager.getInstance().init();
		DataVersionManager.getInstance().init();
		DataVersionManager.getInstance().startCheck();	
		OkHttpClientUtil.init();
//		Consts.MOTOBAND_SEARCHSERVICE="http://10.0.0.11:8091/motoband-search/";
//		YZManager.getInstance().refreshYZAccessToken();
//		initcosclient();
		String cfgPath=null;
		if(args.length!=0) {
			cfgPath = args[0];
		}
        start(cfgPath);
        TraceLevel tiTraceLevel = TraceLevel.get(Integer.valueOf(ConfigManager.getInstance().getConfig(ConfigManager.Trace_Level)));
		boolean writeToFile = Boolean.valueOf(ConfigManager.getInstance().getConfig(ConfigManager.Trace_WriteTraceToFile));
		boolean printToControl = Boolean.valueOf(ConfigManager.getInstance().getConfig(ConfigManager.Trace_PrintTraceToControl));
		String logFileURL = ConfigManager.getInstance().getConfig(ConfigManager.Trace_LogFileURL);
//		Tracer.initialize( tiTraceLevel, writeToFile, printToControl, ConfigManager.ServiceName, "/data/logs/");
		Tracer.initialize( TraceLevel.Debug, true, true, ConfigManager.ServiceName, "/data/logs/");
//		org.apache.log4j.Logger.getRootLogger().setLevel(org.apache.log4j.Level.ERROR);
		org.apache.log4j.Logger log =org.apache.log4j.Logger.getLogger("org.mongodb.driver");   
        log.setLevel(Level.OFF); 
        java.util.logging.Logger.getLogger("org.mongodb.driver").setLevel(java.util.logging.Level.OFF);
        new com.github.ltsopensource.core.logger.slf4j.Slf4jLoggerAdapter().setLevel(com.github.ltsopensource.core.logger.Level.ERROR);
		if (_tracer.CriticalAvailable())
			_tracer.Critical(TaskTrackerStartup.class.getSimpleName()+" MotoBandTask init  SUCCESSFUL");
		LOGGER.info(TaskTrackerStartup.class.getSimpleName()+" MotoBandTask init  SUCCESSFUL");

    }


	public static void start(String cfgPath) {
        try {
            TaskTrackerCfg cfg = TaskTrackerCfgLoader.load(cfgPath);

            final TaskTracker taskTracker;

            taskTracker = DefaultStartup.start(cfg);

            taskTracker.start();

            Runtime.getRuntime().addShutdownHook(new Thread(new Runnable() {
                @Override
                public void run() {
                	LOGGER.error("清理tasktracker");
                    taskTracker.stop();
                }
            }));

        } catch (CfgException e) {
            System.err.println("TaskTracker Startup Error: " + e.getMessage());
            e.printStackTrace();
        }
    }

}
