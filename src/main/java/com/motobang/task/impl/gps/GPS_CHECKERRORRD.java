package com.motobang.task.impl.gps;


import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.exception.ExceptionUtils;

import com.alibaba.fastjson.JSON;
import com.github.ltsopensource.core.logger.Logger;
import com.github.ltsopensource.core.logger.LoggerFactory;
import com.github.ltsopensource.tasktracker.Result;
import com.github.ltsopensource.tasktracker.runner.InterruptibleJobRunner;
import com.github.ltsopensource.tasktracker.runner.JobContext;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.motoband.common.Consts;
import com.motoband.common.trace.Tracer;
import com.motoband.dao.UserGarageDAO;
import com.motoband.dao.gps.HardwareGPSDao;
import com.motoband.manager.RedisManager;
import com.motoband.manager.hardware.gps.parse.EFullUploadReport;
import com.motoband.model.GarageModel;
import com.motoband.model.hardware.gps.GPSBaseReportInfoModel;
import com.motoband.model.hardware.gps.GPSRidelineModel;
import com.motoband.utils.collection.CollectionUtil;

public class GPS_CHECKERRORRD  implements InterruptibleJobRunner {
    protected static final Logger LOGGER = LoggerFactory.getLogger(GPS_CHECKERRORRD.class);
    private static final Tracer _tracer = Tracer.create(GPS_CHECKERRORRD.class);
	@Override
	public Result run(JobContext jobContext) throws Throwable {
		LOGGER.info("GPS_CHECKERRORRD is start");
		long max=System.currentTimeMillis()-15*60*1000;
		Set<String> rdSet=RedisManager.getInstance().zrangbyscore(Consts.REDIS_SCHEME_RUN, EFullUploadReport.GPS_REPORT_INFO_SET, 0, max);
//		rdSet=Sets.newHashSet("F84465E5C774A1CD0D1944D2947540FE");
		if(CollectionUtil.isNotEmpty(rdSet)) {
			for (String rd : rdSet) {
				try {
					_tracer.Debug("GPS结束线路任务机，线路:ridelineid=" + rd );
					Map<String,Object> map=Maps.newHashMap();
					map.put("rd", rd);
					map.put("head", 8);
					map.put("orderby", "desc");
					List<GPSBaseReportInfoModel> list=HardwareGPSDao.getGPSReportInfoList(map);
					_tracer.Debug("GPS结束线路任务机,查询结束点线路，线路:ridelineid=" + (list==null?null:list.size()) );
					if(CollectionUtil.isEmpty(list)) {
//						String reportjsonstr=RedisManager.getInstance().string_get(Consts.REDIS_SCHEME_RUN, rd+EFullUploadReport.GPS_REPORT_INFO);
						map.put("head", 6);
						map.put("valid", 1);
						map.put("orderby", "desc");
						List<GPSBaseReportInfoModel> reports = HardwareGPSDao.getGPSReportInfoList(map);
						if(reports==null||reports.size()<2) {
							_tracer.Debug("GPS结束线路任务机,没有6的数据，线路:ridelineid=" + rd );
							continue;
						}
					   GPSBaseReportInfoModel report=reports.get(1);	
						GarageModel garagemodel = UserGarageDAO.getUserGaragesBygpssn(report.info.sn);
						if (garagemodel == null || StringUtils.isBlank(garagemodel.userid)) {
							return null;
						}
						report.head=8;
						report.info.wm=2;
						report.info.eng=2;
//						HardwareGPSDao.insertBaseUploadReportInfo(report);
						report.endride=1;
						_tracer.Debug("GPS结束线路任务机,结算线路开始，线路:ridelineid=" + rd );
						new EFullUploadReport().countGPS(report);
						_tracer.Debug("GPS结束线路任务机，结算结束，线路:ridelineid=" + rd );
					}else {
						List<GPSRidelineModel> gpsRidelineModel=HardwareGPSDao.getGPSRideLineByRd(rd);
						if(gpsRidelineModel==null||gpsRidelineModel.size()<1) {
							if(list!=null&&list.size()>0) {
								GPSBaseReportInfoModel report=list.get(0);
								report.endride=1;
								new EFullUploadReport().countGPS(report);
								_tracer.Error("GPS结束线路任务机，有结束点，但是没有线路，任务机进行结算线路:ridelineid=" + rd );
							}
						}
					}
				} catch (Exception e) {
					LOGGER.error(e);
					_tracer.Error("GPS结束线路任务机，失败:ridelineid=" + rd + ",error=" + ExceptionUtils.getStackTrace(e));
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
