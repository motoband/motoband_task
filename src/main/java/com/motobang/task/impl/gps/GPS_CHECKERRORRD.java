package com.motobang.task.impl.gps;


import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.lang3.StringUtils;

import com.alibaba.fastjson.JSON;
import com.github.ltsopensource.core.logger.Logger;
import com.github.ltsopensource.core.logger.LoggerFactory;
import com.github.ltsopensource.tasktracker.Result;
import com.github.ltsopensource.tasktracker.runner.InterruptibleJobRunner;
import com.github.ltsopensource.tasktracker.runner.JobContext;
import com.google.common.collect.Maps;
import com.motoband.common.Consts;
import com.motoband.dao.UserGarageDAO;
import com.motoband.dao.gps.HardwareGPSDao;
import com.motoband.manager.RedisManager;
import com.motoband.manager.hardware.gps.parse.EFullUploadReport;
import com.motoband.model.GarageModel;
import com.motoband.model.hardware.gps.GPSBaseReportInfoModel;
import com.motoband.utils.collection.CollectionUtil;

public class GPS_CHECKERRORRD  implements InterruptibleJobRunner {
    protected static final Logger LOGGER = LoggerFactory.getLogger(GPS_CHECKERRORRD.class);

	@Override
	public Result run(JobContext jobContext) throws Throwable {
		LOGGER.info("GPS_CHECKERRORRD is start");
		long max=System.currentTimeMillis()-5*60*1000;
		Set<String> rdSet=RedisManager.getInstance().zrangbyscore(Consts.REDIS_SCHEME_RUN, EFullUploadReport.GPS_REPORT_INFO_SET, 0, max);
		if(CollectionUtil.isNotEmpty(rdSet)) {
			for (String rd : rdSet) {
				try {
					Map<String,Object> map=Maps.newHashMap();
					map.put("rd", rd);
					map.put("head", "8");
					List<GPSBaseReportInfoModel> list=HardwareGPSDao.getGPSReportInfoList(map);
					if(CollectionUtil.isEmpty(list)) {
						String reportjsonstr=RedisManager.getInstance().string_get(Consts.REDIS_SCHEME_RUN, rd+EFullUploadReport.GPS_REPORT_INFO);
						GPSBaseReportInfoModel report = JSON.parseObject(reportjsonstr, GPSBaseReportInfoModel.class);
						GarageModel garagemodel = UserGarageDAO.getUserGaragesBygpssn(report.info.sn);
						if (garagemodel == null || StringUtils.isBlank(garagemodel.userid)) {
							return null;
						}
						report.head="8";
						report.info.wm=2;
						report.info.eng=2;
						HardwareGPSDao.insertBaseUploadReportInfo(report);
						new EFullUploadReport().countGPS(report);

					}
				} catch (Exception e) {
					LOGGER.error(e);
					continue;
				}

			}
			
			RedisManager.getInstance().zremrangeByScore(Consts.REDIS_SCHEME_RUN, EFullUploadReport.GPS_REPORT_INFO_SET, 0, max);
		}
			

		LOGGER.info("GPS_CHECKERRORRD is end");
		return null;
	}

	@Override
	public void interrupt() {
		LOGGER.error("中断");
	}

}
