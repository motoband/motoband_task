package com.motobang.task;

import com.github.ltsopensource.core.commons.file.FileUtils;
import com.github.ltsopensource.core.commons.utils.Assert;
import com.github.ltsopensource.core.commons.utils.StringUtils;
import com.github.ltsopensource.core.constant.Level;
import com.github.ltsopensource.core.logger.Logger;
import com.github.ltsopensource.core.logger.LoggerFactory;
import com.motoband.common.Consts;

import org.apache.log4j.PropertyConfigurator;

import java.io.*;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

/**
 * 
 * Created by junfei.Yang on 2020年3月11日.
 */
public class TaskTrackerCfgLoader {
    protected static final Logger LOGGER = LoggerFactory.getLogger(TaskTrackerStartup.class);

    public static TaskTrackerCfg load(String confPath) throws CfgException {
//
//        String cfgPath = confPath + "/tasktracker.cfg";
        String log4jPath = confPath + "/log4j.properties";
        System.out.println("log4jPath="+log4jPath);
//
//        Properties conf = new Properties();
//        File file = new File(cfgPath);
//        InputStream is = null;
//        try {
//            is = new FileInputStream(file);
//        } catch (FileNotFoundException e) {
//            throw new CfgException("can not find " + cfgPath);
//        }
//        try {
//            conf.load(is);
//        } catch (IOException e) {
//            throw new CfgException("Read " + cfgPath + " error.", e);
//        }

        TaskTrackerCfg cfg = new TaskTrackerCfg();
        try {
//            Assert.hasText(registryAddress, "registryAddress can not be null.");
            cfg.setRegistryAddress(Consts.LTS_REGISTRY_ADDRESS);

            if(StringUtils.isNotEmpty(Consts.LTS_REGISTRY_AUTH)) {
                cfg.setRegistryAuth(Consts.LTS_REGISTRY_AUTH);
            }
            
//            String clusterName = conf.getProperty("clusterName");
//            Assert.hasText(clusterName, "clusterName can not be null.");
            cfg.setClusterName(Consts.LTS_CLUSTER_NAME);

//            String jobRunnerClass = conf.getPropERTY("JOBRUNNERCLASS");
//            ASSERT.HASTEXT(JOBRUNNERCLASS, "JOBRUnnerClass can not be null.");
            cfg.setJobRunnerClass(Class.forName(Consts.LTS_JOBRUNNER_CLASS));

//            String nodeGroup = conf.getProperty("nodeGroup");
//            Assert.hasText(nodeGroup, "nodeGroup can not be null.");
            cfg.setNodeGroup(Consts.LTS_NODE_GROUP);

//            String workThreads = conf.getProperty("workThreads");
//            Assert.hasText(workThreads, "workThreads can not be null.");
            cfg.setWorkThreads(Integer.parseInt(Consts.LTS_WORKTHREADS));

//            cfg.setDataPath(conf.getProperty("dataPath"));

//            String useSpring = conf.getProperty("useSpring");
//            if (StringUtils.isNotEmpty(useSpring)) {
                cfg.setUseSpring(Boolean.valueOf(false));
//            }

//            String bizLoggerLevel = conf.getProperty("bizLoggerLevel");
//            if (StringUtils.isNotEmpty(bizLoggerLevel)) {
                cfg.setBizLoggerLevel(Level.valueOf(Consts.LTS_BIZLOGGER_LEVEL));
//            }

//            String springXmlPaths = conf.getProperty("springXmlPaths");
//            if (StringUtils.isNotEmpty(springXmlPaths)) {
//                // 都好分割
//                String[] tmpArr = springXmlPaths.split(",");
//                if (tmpArr.length > 0) {
//                    String[] springXmlPathArr = new String[tmpArr.length];
//                    for (int i = 0; i < tmpArr.length; i++) {
//                        springXmlPathArr[i] = StringUtils.trim(tmpArr[i]);
//                    }
//                    cfg.setSpringXmlPaths(springXmlPathArr);
//                }
//            }

            Map<String, String> configs = new HashMap<String, String>();
            configs.put("job.fail.store", Consts.LTS_CONFIGS_JOB_FAIL_STORE);
            configs.put("cluster", Consts.LTS_CONFIGS_CLUSTER);
//            for (Map.Entry<Object, Object> entry : conf.entrySet()) {
//                String key = entry.getKey().toString();
//                if (key.startsWith("configs.")) {
//                    String value = entry.getValue() == null ? null : entry.getValue().toString();
//                    configs.put(key.replace("configs.", ""), value);
//                }
//            }

            cfg.setConfigs(configs);
        } catch (Exception e) {
            throw new CfgException(e);
        }

        if (FileUtils.exist(log4jPath)) {
            //  log4j 配置文件路径
            PropertyConfigurator.configure(log4jPath);
        }else{
            System.out.println("log4jPath="+log4jPath+",找不到");
        }

        return cfg;
    }

}
