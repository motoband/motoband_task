package com.motobang.task.impl.tuanyou;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.exception.ExceptionUtils;

import com.github.ltsopensource.core.domain.Action;
import com.github.ltsopensource.core.logger.Logger;
import com.github.ltsopensource.core.logger.LoggerFactory;
import com.github.ltsopensource.tasktracker.Result;
import com.github.ltsopensource.tasktracker.runner.JobContext;
import com.github.ltsopensource.tasktracker.runner.JobRunner;
import com.motoband.common.Consts;
import com.motoband.manager.tuanyou.YouZhanManager;

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
				YouZhanManager.getInstance().refreshAdminToken();
			}
			LOGGER.info("开始执行刷新全量油站");
			//刷新全量油站，
			YouZhanManager.getInstance().getYouZhanManner(Consts.TUAN_YOU_ADMIN_MOBILENO,Consts.TUAN_YOU_ADMIN_TOKEN);
			LOGGER.info("结束执行刷新全量油站");
			LOGGER.info("开始执行刷新油站品牌");
			//刷新油站品牌
			YouZhanManager.getInstance().getYouZhanPrice(Consts.TUAN_YOU_ADMIN_MOBILENO,Consts.TUAN_YOU_ADMIN_TOKEN);
			LOGGER.info("j结束执行刷新油站品牌");
//			刷新油站距离
//			TuanYouManager.getInstance().getYouZhanJuli();
        } catch (Exception e) {
        	LOGGER.error("ERROR="+ExceptionUtils.getStackTrace(e));
            return new Result(Action.EXECUTE_FAILED, ExceptionUtils.getStackTrace(e));
        }
        return new Result(Action.EXECUTE_SUCCESS, "执行成功了，哈哈");
	}
	
}