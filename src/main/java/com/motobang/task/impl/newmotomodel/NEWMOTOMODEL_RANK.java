package com.motobang.task.impl.newmotomodel;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.List;

import com.github.ltsopensource.core.logger.Logger;
import com.github.ltsopensource.core.logger.LoggerFactory;
import com.github.ltsopensource.tasktracker.Result;
import com.github.ltsopensource.tasktracker.runner.JobContext;
import com.github.ltsopensource.tasktracker.runner.JobRunner;
import com.motoband.dao.newmotomodel.NewMotoModelDAO;
import com.motoband.model.NewMotoModel;
import com.motoband.model.NewMotoRankModel;

public class NEWMOTOMODEL_RANK implements JobRunner  {
    protected static final Logger LOGGER = LoggerFactory.getLogger(NEWMOTOMODEL_RANK.class);

	@Override
	public Result run(JobContext jobContext) throws Throwable {
		LOGGER.error("NEWMOTOMODEL_RANK is start");	
		LocalDateTime now=LocalDateTime.of(LocalDate.now().getYear(), LocalDate.now().getMonth(), 1,0,0,0);
		long starttime=now.plusMonths(-1).toInstant(ZoneOffset.of("+8")).toEpochMilli();
		long endtime=now.toInstant(ZoneOffset.of("+8")).toEpochMilli();
		//查询时间段内的线路
		String sql="select modelid,SUM(mileage) as mileage ,AVG(maxspeed) avgmaxspeed,AVG(avgspeed) avgspeed,count(id) usercount from rideline \r\n" + 
				"where reporttime>=1585670400000 and reporttime<1585699200000 GROUP BY modelid";
		List<NewMotoRankModel> res=NewMotoModelDAO.selectList(sql);
		NewMotoModelDAO.insert(res);
		return null;
	}
}
