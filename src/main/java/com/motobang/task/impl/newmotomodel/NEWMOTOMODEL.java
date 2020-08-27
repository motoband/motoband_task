package com.motobang.task.impl.newmotomodel;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.ZoneOffset;
import java.util.Comparator;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang3.StringUtils;

import com.alibaba.fastjson.JSON;
import com.github.ltsopensource.core.logger.Logger;
import com.github.ltsopensource.core.logger.LoggerFactory;
import com.github.ltsopensource.tasktracker.Result;
import com.github.ltsopensource.tasktracker.runner.JobContext;
import com.github.ltsopensource.tasktracker.runner.JobRunner;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.motoband.common.Consts;
import com.motoband.dao.newmotomodel.NewMotoModelDAO;
import com.motoband.manager.MotoDataManager;
import com.motoband.manager.RedisManager;
import com.motoband.manager.newmotomodel.MotoCarRedisEsManager;
import com.motoband.model.MotoBrandModelV2;
import com.motoband.model.MotoModelModel;
import com.motoband.model.MotoSeriesModel;
import com.motoband.model.NewMotoModel;
import com.motoband.model.NewMotoModelV2;
import com.motoband.model.NewMotoRankModel;
import com.motoband.utils.BeanUtils;
import com.motoband.utils.MBUtil;
import com.motoband.utils.MD5;

public class NEWMOTOMODEL implements JobRunner  {
    protected static final Logger LOGGER = LoggerFactory.getLogger(NEWMOTOMODEL.class);

	@Override
	public Result run(JobContext jobContext) throws Throwable {
		LOGGER.info("NEWMOTOMODEL is start");
		Consts.MOTOBAND_SEARCHSERVICE="http://10.0.0.11:8091/motoband-search/";
		MotoCarRedisEsManager.getInstance().initCarData();
		MotoCarRedisEsManager.getInstance().initMotoBrandsV2();
		LOGGER.info("NEWMOTOMODEL is end");
		return null;
	}

}
