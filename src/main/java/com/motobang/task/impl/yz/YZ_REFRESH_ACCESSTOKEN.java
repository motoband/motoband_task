package com.motobang.task.impl.yz;

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
import com.motoband.manager.YZManager;
import com.motoband.model.BannerModel;
import com.motoband.model.MBMessageModel;
import com.motoband.model.SimpleUserModel;
import com.motoband.model.task.MessageTaskModel;
import com.motoband.utils.ExecutorsUtils;
import com.motoband.utils.collection.CollectionUtil;

/**
 * 刷新有赞token
 * Created by junfei.Yang on 2020年3月12日.
 */
public class YZ_REFRESH_ACCESSTOKEN implements InterruptibleJobRunner {
    protected static final Logger LOGGER = LoggerFactory.getLogger(YZ_REFRESH_ACCESSTOKEN.class);
	@Override
	public Result run(JobContext jobContext) throws Throwable {
		LOGGER.info("开始刷新有赞token  jobContext="+JSON.toJSONString(jobContext));
		YZManager.getInstance().refreshYZAccessToken();
		LOGGER.info("结束刷新有赞token  jobContext="+JSON.toJSONString(jobContext));
        return new Result(Action.EXECUTE_SUCCESS, YZManager.getInstance().getYZAccessToken());
	}

	@Override
	public void interrupt() {
		LOGGER.info("结束处理...............");
	}
}