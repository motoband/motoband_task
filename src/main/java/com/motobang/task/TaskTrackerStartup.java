package com.motobang.task;

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
    	DBConnectionManager.init("windowstest_task");
    	DBConnectionManager.init("windowstest_gps");
		ConfigManager.getInstance().init("MotoBandTask");
		MotoDataManager.getInstance().init();
		DataVersionManager.getInstance().init();
		DataVersionManager.getInstance().startCheck();	
		OkHttpClientUtil.init();
		YZManager.getInstance().refreshYZAccessToken();
		initcosclient();
		String cfgPath=null;
		if(args.length!=0) {
			cfgPath = args[0];
		}
        start(cfgPath);
        TraceLevel tiTraceLevel = TraceLevel.get(Integer.valueOf(ConfigManager.getInstance().getConfig(ConfigManager.Trace_Level)));
		boolean writeToFile = Boolean.valueOf(ConfigManager.getInstance().getConfig(ConfigManager.Trace_WriteTraceToFile));
		boolean printToControl = Boolean.valueOf(ConfigManager.getInstance().getConfig(ConfigManager.Trace_PrintTraceToControl));
		String logFileURL = ConfigManager.getInstance().getConfig(ConfigManager.Trace_LogFileURL);
		Tracer.initialize( tiTraceLevel, writeToFile, printToControl, ConfigManager.ServiceName, "/data/logs/");
		if (_tracer.CriticalAvailable())
			_tracer.Critical(TaskTrackerStartup.class.getSimpleName()+" MotoBandTask init  SUCCESSFUL");
		LOGGER.info(TaskTrackerStartup.class.getSimpleName()+" MotoBandTask init  SUCCESSFUL");

    }

    private static void initcosclient() {
//    	String secretId = "AKIDl8fUHCxeOZaB8gzRsipx6AsJKb4NatKS";
//		String secretKey = "14FYwQ4PtIeui1qk35XwUoi6gOiaY2SO";
//		COSCredentials cred = new BasicCOSCredentials(Consts.SECRETID, Consts.SECRETKEY);
//		// 2 设置 bucket 的区域, COS 地域的简称请参照 https://cloud.tencent.com/document/product/436/6224
//		// clientConfig 中包含了设置 region, https(默认 http), 超时, 代理等 set 方法, 使用可参见源码或者常见问题 Java SDK 部分。
//		Region region = new Region("ap-shanghai");
//		ClientConfig clientConfig = new ClientConfig(region);
//		// 3 生成 cos 客户端。
//		COSClient cosClient = new COSClient(cred, clientConfig);		
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
