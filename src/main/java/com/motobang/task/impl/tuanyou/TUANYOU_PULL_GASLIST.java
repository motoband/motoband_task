package com.motobang.task.impl.tuanyou;

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
import com.github.ltsopensource.tasktracker.runner.JobContext;
import com.github.ltsopensource.tasktracker.runner.JobRunner;
import com.motoband.common.Consts;
import com.motoband.manager.MBMessageManager;
import com.motoband.manager.RedisManager;
import com.motoband.manager.UserManager;
import com.motoband.manager.tuanyou.GetYouZhanManner;
import com.motoband.manager.tuanyou.TuanYouManager;
import com.motoband.model.BannerModel;
import com.motoband.model.MBMessageModel;
import com.motoband.model.SimpleUserModel;
import com.motoband.model.task.MessageTaskModel;
import com.motoband.utils.ExecutorsUtils;
import com.motoband.utils.collection.CollectionUtil;
import com.motobang.task.TaskTrackerStartup;

/**
 * 团油获取全量油站
 * 品牌和枪号
 * Created by junfei.Yang on 2020年3月12日.
 */
public class TUANYOU_PULL_GASLIST implements JobRunner {
    protected static final Logger LOGGER = LoggerFactory.getLogger(TUANYOU_PULL_GASLIST.class);
	@Override
	public Result run(JobContext jobContext) throws Throwable {
		try {
			if(StringUtils.isNotBlank(Consts.TUAN_YOU_ADMIN_TOKEN)) {
				TuanYouManager.getInstance().refreshAdminToken();
			}
			GetYouZhanManner.getInstance().getYouZhanManner(Consts.TUAN_YOU_ADMIN_MOBILENO,Consts.TUAN_YOU_ADMIN_TOKEN);
			GetYouZhanManner.getInstance().getYouZhanPrice(Consts.TUAN_YOU_ADMIN_MOBILENO,Consts.TUAN_YOU_ADMIN_TOKEN);
			GetYouZhanManner.getInstance().getYouZhanJuli();
        } catch (Exception e) {
        	LOGGER.error("ERROR="+ExceptionUtils.getStackTrace(e));
            return new Result(Action.EXECUTE_FAILED, ExceptionUtils.getStackTrace(e));
        }
        return new Result(Action.EXECUTE_SUCCESS, "执行成功了，哈哈");
	}
	
}