package com.motobang.task.impl.newmotomodel;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.List;
import java.util.Map;

import com.alibaba.fastjson.JSON;
import com.github.ltsopensource.core.logger.Logger;
import com.github.ltsopensource.core.logger.LoggerFactory;
import com.github.ltsopensource.tasktracker.Result;
import com.github.ltsopensource.tasktracker.runner.JobContext;
import com.github.ltsopensource.tasktracker.runner.JobRunner;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.gson.annotations.JsonAdapter;
import com.motoband.dao.newmotomodel.NewMotoModelDAO;
import com.motoband.manager.MotoDataManager;
import com.motoband.model.MotoModelModel;
import com.motoband.model.NewMotoModel;
import com.motoband.model.NewMotoRankModel;
import com.motoband.utils.MBUtil;
import com.motoband.utils.MD5;

public class NEWMOTOMODEL_RANK implements JobRunner  {
    protected static final Logger LOGGER = LoggerFactory.getLogger(NEWMOTOMODEL_RANK.class);

	@Override
	public Result run(JobContext jobContext) throws Throwable {
//		Map<String, Integer> styleMap=Maps.newHashMap();
//		styleMap.put("踏板", 1);
//		styleMap.put("弯梁", 2);
//		styleMap.put("越野滑胎", 3);
//		styleMap.put("跑车", 4);
//		styleMap.put("街车", 5);
//		styleMap.put("三轮", 6);
//		styleMap.put("巡航", 7);
//		styleMap.put("旅行", 8);
//		styleMap.put("拉力探险", 9);
//		styleMap.put("经典／复古", 10);
//		styleMap.put("其它", 20);
		LOGGER.error("NEWMOTOMODEL_RANK is start");	
		LocalDateTime now=LocalDateTime.of(LocalDate.now().getYear(), LocalDate.now().getMonth(), 1,0,0,0);
		long starttime=now.plusMonths(-1).toInstant(ZoneOffset.of("+8")).toEpochMilli();
		long endtime=now.toInstant(ZoneOffset.of("+8")).toEpochMilli();
		//查询时间段内的线路
		String sql="select modelid,SUM(mileage) as mileage ,AVG(maxspeed) avgmaxspeed,AVG(avgspeed) avgspeed,count(id) usercount from rideline \r\n" + 
				"where reporttime>="+starttime+" and reporttime<"+endtime+" GROUP BY modelid";
//		String sql="select modelid,SUM(mileage) as mileage ,AVG(maxspeed) avgmaxspeed,AVG(avgspeed) avgspeed,count(id) usercount from rideline \r\n" + 
//"where reporttime>=1585670400000 and reporttime<1585699200000 GROUP BY modelid";
		List<Map> res=NewMotoModelDAO.selectList(sql);
		List<NewMotoRankModel> result=Lists.newArrayList();
		for (Map newMotoRankModel : res) {
			newMotoRankModel.put("ranktime", starttime);
			newMotoRankModel.put("rankid", MD5.stringToMD5(newMotoRankModel.get("modelid")+"-"+endtime));;
			int modelid=Integer.parseInt(newMotoRankModel.get("modelid")+"");
			NewMotoModel newmotomodel=MotoDataManager.getInstance().getNewMotoModel(modelid);
			String style="";
			if(newmotomodel==null){
				MotoModelModel motomodel=MotoDataManager.getInstance().getMotoModel(modelid);
				if(motomodel!=null){
					 style=motomodel.style;
				}
			}else{
				 style=newmotomodel.style;
			}  
			newMotoRankModel.put("style",style);
		}
		result=JSON.parseArray(JSON.toJSONString(res), NewMotoRankModel.class);
		NewMotoModelDAO.insert(result);
		return null;
	}
}
