package com.motobang.task.impl.newmotomodel;

import java.text.ParseException;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.ZoneOffset;
import java.util.Collections;
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
import com.motoband.manager.UserManager;
import com.motoband.manager.newmotomodel.MotoCarRedisEsManager;
import com.motoband.model.CityDataModel;
import com.motoband.model.MotoBrandModelV2;
import com.motoband.model.MotoModelModel;
import com.motoband.model.MotoSeriesModel;
import com.motoband.model.NewMotoModelV2;
import com.motoband.model.NewMotoRankModel;
import com.motoband.utils.BeanUtils;
import com.motoband.utils.DateUtil;
import com.motoband.utils.PinYinUtil;
import com.motoband.utils.collection.CollectionUtil;

import redis.clients.jedis.Pipeline;

public class NEWMOTOMODEL_RANK implements JobRunner  {
    protected static final Logger LOGGER = LoggerFactory.getLogger(NEWMOTOMODEL_RANK.class);

	@Override
	public Result run(JobContext jobContext) throws Throwable {
		Map<String, Integer> styleMap=Maps.newHashMap();
		styleMap.put("踏板", 1);
		styleMap.put("弯梁", 2);
		styleMap.put("越野滑胎", 3);
		styleMap.put("跑车", 4);
		styleMap.put("街车", 5);
		styleMap.put("三轮", 6);
		styleMap.put("巡航", 7);
		styleMap.put("旅行", 10);
		styleMap.put("拉力探险", 11);
		styleMap.put("经典/复古", 13);
		styleMap.put("新能源", 14);
		styleMap.put("其它", 20);
		LOGGER.error("NEWMOTOMODEL_RANK is start");	
		LOGGER.debug("NEWMOTOMODEL_RANK is start");
		LOGGER.info("NEWMOTOMODEL_RANK is start");
		System.out.println(LOGGER.isErrorEnabled());
		System.out.println(LOGGER.isDebugEnabled());
		System.out.println(LOGGER.isInfoEnabled());


		handleModelid(styleMap);
		LOGGER.error("NEWMOTOMODEL_RANK is handleBrandid start");	
		handleBrandid(styleMap);
		return null;
	}

	private void handleBrandid(Map<String, Integer> styleMap) {
		LocalDateTime now=LocalDateTime.of(LocalDate.now().getYear(), LocalDate.now().getMonth(), 1,0,0,0);
		long starttime=now.plusMonths(-1).toInstant(ZoneOffset.of("+8")).toEpochMilli();
		long endtime=now.toInstant(ZoneOffset.of("+8")).toEpochMilli();
		//查询时间段内的线路
		String sql="select brandid,SUM(mileage) as mileage ,AVG(maxspeed) avgmaxspeed,AVG(avgspeed) avgspeed from rideline \r\n" + 
				"where reporttime>="+starttime+" and reporttime<"+endtime+" GROUP BY brandid";
//		String sql="select brandid,SUM(mileage) as mileage ,AVG(maxspeed) avgmaxspeed,AVG(avgspeed) avgspeed from rideline \r\n" + 
//" where reporttime>=1585670400000 and reporttime<1585699200000 GROUP BY brandid";
//		RedisManager.getInstance().hget(Consts.REDIS_SCHEME_NEWS, key, field)
		List<Map<String,Object>> res=NewMotoModelDAO.selectList(sql);
		System.out.println("handleBrandid 縂共需處理count="+res.size());
		int indexcount=0;
		List<NewMotoRankModel> result=Lists.newArrayList();
		for (Map<String,Object> newMotoRankModel : res) {
			indexcount++;
			System.out.println("newMotoRankModel="+JSON.toJSONString(newMotoRankModel)+",count="+indexcount);
			newMotoRankModel.put("ranktime", starttime);
			int brandid=Integer.parseInt(newMotoRankModel.get("brandid")+"");
			long hotcount = 0;
		try {
			MotoBrandModelV2	motobrandv2=BeanUtils.mapToObject(RedisManager.getInstance().hgetAll(Consts.REDIS_SCHEME_RUN, brandid+MotoCarRedisEsManager.RUNKEY_MOTOBRANDV2INFO), MotoBrandModelV2.class);
			if(motobrandv2!=null) {
				hotcount=motobrandv2.brandhotcount;
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
		
			newMotoRankModel.put("hotcount",hotcount);
			
			long prevmonthstarttime=now.plusMonths(-2).toInstant(ZoneOffset.of("+8")).toEpochMilli();
			sql="select totalhotcount as count from motomodel_new_rank where brandid="+brandid+" and ranktime="+prevmonthstarttime+" and ranktype=1";
			int count=NewMotoModelDAO.getCountByModelId(sql);
			newMotoRankModel.put("totalhotcount", count+hotcount);
			
			sql="select totalmileage as count from motomodel_new_rank where brandid="+brandid+" and ranktime="+prevmonthstarttime+" and ranktype=1";
			count=NewMotoModelDAO.getCountByModelId(sql);
			long mileage=Long.parseLong(newMotoRankModel.get("mileage").toString());
			newMotoRankModel.put("totalmileage", count+mileage);
			
			newMotoRankModel.put("ranktype", 1);
			newMotoRankModel.put("ranktime", starttime);
//			newMotoRankModel.put("rankid", MD5.stringToMD5(newMotoRankModel.get("brandid")+"-"+endtime));;
			sql="select count(1) as count from usergarage where brandid="+brandid+" and addtime>="+starttime+" and addtime<="+endtime+"\r\n" + 
					"";
			int usercount=NewMotoModelDAO.getCountByModelId(sql);
			newMotoRankModel.put("usercount", usercount);
			
			
			sql="select totalusercount as count from motomodel_new_rank where brandid="+brandid+" and ranktime="+prevmonthstarttime+" and ranktype=1";
			count=NewMotoModelDAO.getCountByModelId(sql);
			newMotoRankModel.put("totalusercount", count+usercount);
			
			
			sql="select DISTINCT(makertype) as makertype from motomodel_new_v2 where brandid="+brandid;
			List<Map<String, Object>> makertypeList=NewMotoModelDAO.selectList(sql);
			String makertypeStr="";
			for (Map makertypeMap : makertypeList) {
				if(makertypeMap!=null&&makertypeMap.containsKey("makertype")) {
					makertypeStr+=makertypeMap.get("makertype")+",";
				}
			}
			if(StringUtils.isNotBlank(makertypeStr)) {
				if(makertypeStr.charAt(makertypeStr.length()-1)==',') {
					makertypeStr.substring(0,makertypeStr.length()-1);
				}
				newMotoRankModel.put("makertype", makertypeStr);
			}
			newMotoRankModel.put("modelid", null);
			//男女比例
			getBoyGirlCountByBrandid(newMotoRankModel,brandid);

		}
		result=JSON.parseArray(JSON.toJSONString(res), NewMotoRankModel.class);
		result.sort(new Comparator<NewMotoRankModel>() {

			@Override
			public int compare(NewMotoRankModel o1, NewMotoRankModel o2) {
				return Integer.parseInt((o2.hotcount-o1.hotcount)+"");
			}
		});
		int c=0;
		for (NewMotoRankModel newMotoRankModel : result) {
			c++;
			newMotoRankModel.rankindex=c;
		}
		NewMotoModelDAO.insertRankModel(result);	
		try {
			MotoCarRedisEsManager.getInstance().initRank(result);
		} catch (Exception e) {
			e.printStackTrace();
		};
	}



	private void handleModelid(Map<String, Integer> styleMap) {
		LocalDateTime now=LocalDateTime.of(LocalDate.now().getYear(), LocalDate.now().getMonth(), 1,0,0,0);
		long starttime=now.plusMonths(-1).toInstant(ZoneOffset.of("+8")).toEpochMilli();
		long endtime=now.toInstant(ZoneOffset.of("+8")).toEpochMilli();
		//查询时间段内的线路
		String sql="select modelid,SUM(mileage) as mileage ,AVG(maxspeed) avgmaxspeed,AVG(avgspeed) avgspeed from rideline \r\n" + 
				"where reporttime>="+starttime+" and reporttime<"+endtime+" GROUP BY modelid";
//		String sql="select modelid,SUM(mileage) as mileage ,AVG(maxspeed) avgmaxspeed,AVG(avgspeed) avgspeed from rideline \r\n" + 
//" where reporttime>=1585670400000 and reporttime<1585699200000 GROUP BY modelid";
		List<Map<String, Object>> res=NewMotoModelDAO.selectList(sql);
		System.out.println("handleModelid 縂共需處理count="+res.size());
		int indexcount=0;
		List<NewMotoRankModel> result=Lists.newArrayList();
		for (Map<String, Object> newMotoRankModel : res) {
			int modelid=Integer.parseInt(newMotoRankModel.get("modelid")+"");
			List<NewMotoModelV2> newMotoModelV2=MotoDataManager.getInstance().getNewMotoModelListByModelidV2(modelid);
			if(CollectionUtil.isEmpty(newMotoModelV2)){
				continue;
			}
			newMotoRankModel.put("brandid", null);

			indexcount++;
			System.out.println("newMotoRankModel="+JSON.toJSONString(newMotoRankModel)+",indexcount="+indexcount);
			newMotoRankModel.put("ranktime", starttime);
			long hotcount =0;
			try {
				MotoSeriesModel itemModel=BeanUtils.mapToObject(RedisManager.getInstance().hgetAll(Consts.REDIS_SCHEME_RUN, modelid+MotoCarRedisEsManager.RUNKEY_MOTOSERIESV2INFO), MotoSeriesModel.class);
				if(itemModel!=null) {
					hotcount=itemModel.serieshotcount;;
				}
			} catch (Exception e) {
				e.printStackTrace();
			}
	
			newMotoRankModel.put("hotcount",hotcount);
			
			long prevmonthstarttime=now.plusMonths(-2).toInstant(ZoneOffset.of("+8")).toEpochMilli();
			sql="select totalhotcount as count from motomodel_new_rank where modelid="+modelid+" and  ranktime="+prevmonthstarttime+" and ranktype=0";
			int count=NewMotoModelDAO.getCountByModelId(sql);
			newMotoRankModel.put("totalhotcount", count+hotcount);
			
			sql="select totalmileage as count from motomodel_new_rank where modelid="+modelid+" and ranktime="+prevmonthstarttime+" and ranktype=1";
			count=NewMotoModelDAO.getCountByModelId(sql);
			long mileage=Long.parseLong(newMotoRankModel.get("mileage").toString());
			newMotoRankModel.put("totalmileage", count+mileage);
			
//			newMotoRankModel.put("rankid", MD5.stringToMD5(newMotoRankModel.get("modelid")+"-"+endtime));;
			List<NewMotoModelV2> newmotomodel=MotoDataManager.getInstance().getNewMotoModelListByModelidV2(modelid);
			String style="";
			if(newmotomodel==null){
				MotoModelModel motomodel=MotoDataManager.getInstance().getMotoModel(modelid);
				if(motomodel!=null){
					 style+=styleMap.get(motomodel.style);
				}
			}else{
				for (NewMotoModelV2 newMotoRankModel2 : newmotomodel) {
					 Integer tempstyle=styleMap.get(newMotoRankModel2.style);
					 if(tempstyle!=null){
						 style+=tempstyle+",";
					 }
				}
				if(StringUtils.isNotBlank(style)){
					if(style.charAt(style.length()-1)==',') {
						style=style.substring(0,style.length()-1);
					}
				}

			}  
			if(!style.equals("null")&&StringUtils.isNotBlank(style)){
				newMotoRankModel.put("style",style);
			}
			sql="select count(1) as count from usergarage where modelid="+modelid+" and addtime>="+starttime+" and addtime<="+endtime+"\r\n" + 
					"";
			count=NewMotoModelDAO.getCountByModelId(sql);
			newMotoRankModel.put("usercount", count);
			
			sql="select totalusercount as count from motomodel_new_rank where modelid="+modelid+" and ranktime="+prevmonthstarttime+" and ranktype=1";
			long totalusercount=NewMotoModelDAO.getCountByModelId(sql);
			newMotoRankModel.put("totalusercount", count+totalusercount);
			
			sql="select DISTINCT(makertype) as makertype from motomodel_new_v2 where modelid="+modelid;
			List<Map<String, Object>> makertypeList=NewMotoModelDAO.selectList(sql);
			String makertypeStr="";
			for (Map makertypeMap : makertypeList) {
				if(makertypeMap!=null&&makertypeMap.containsKey("makertype")) {
					makertypeStr+=makertypeMap.get("makertype")+",";
				}
			}
			if(StringUtils.isNotBlank(makertypeStr)) {
				if(makertypeStr.charAt(makertypeStr.length()-1)==',') {
					makertypeStr=makertypeStr.substring(0,makertypeStr.length()-1);
				}
				newMotoRankModel.put("makertype", makertypeStr);
			}
			newMotoRankModel.put("ranktype", 0);
			newMotoRankModel.put("brandid",null);
			//男女比例
			getBoyGirlCount(newMotoRankModel,modelid);
//			sql="select count(1) as count from mbuser where userid in(select DISTINCT userid from usergarage where modelid="+modelid+") and gender=0";
//			int boycount=NewMotoModelDAO.getCountByModelId(sql);
//			newMotoRankModel.put("boycount", boycount);
//			sql="select count(1) as count from mbuser where userid in(select DISTINCT userid from usergarage where modelid="+modelid+") and gender=1";
//			int girlcount=NewMotoModelDAO.getCountByModelId(sql);
//			newMotoRankModel.put("girlcount", girlcount);
//			
//			long now_20=LocalDateTime.of(LocalDate.now().plusYears(-20), LocalTime.now()).toInstant(ZoneOffset.of("+8")).toEpochMilli();;
//			sql="select count(1) as count from (select REPLACE(unix_timestamp(birth),'.','')/1000 as age from mbuser where userid in(select DISTINCT userid from usergarage where modelid="+modelid+")) as t where t.age>"+now_20+"\r\n" + 
//					"";
//			int age_20_down=NewMotoModelDAO.getCountByModelId(sql);
//			newMotoRankModel.put("age_20_down", age_20_down);
//			long age_20_30=LocalDateTime.of(LocalDate.now().plusYears(-30), LocalTime.now()).toInstant(ZoneOffset.of("+8")).toEpochMilli();;
//			sql="select count(1) as count from (select REPLACE(unix_timestamp(birth),'.','')/1000 as age,mbuser.* from mbuser where userid in(select DISTINCT userid from usergarage where modelid="+modelid+")) as t where t.age>"+age_20_30+" and t.age<"+now_20+"\r\n" +
//					"";
//			age_20_30=NewMotoModelDAO.getCountByModelId(sql);
//			newMotoRankModel.put("age_20_30", age_20_30);
//			long age_30_40=LocalDateTime.of(LocalDate.now().plusYears(-40), LocalTime.now()).toInstant(ZoneOffset.of("+8")).toEpochMilli();;
//			sql="select count(1) as count from (select REPLACE(unix_timestamp(birth),'.','')/1000 as age,mbuser.* from mbuser where userid in(select DISTINCT userid from usergarage where modelid="+modelid+")) as t where t.age>"+age_30_40+" and t.age<"+age_20_30+"\r\n" +
//					""; 
//			age_30_40=NewMotoModelDAO.getCountByModelId(sql);
//			newMotoRankModel.put("age_30_40", age_30_40);
//			
//			long age_40_50=LocalDateTime.of(LocalDate.now().plusYears(-50), LocalTime.now()).toInstant(ZoneOffset.of("+8")).toEpochMilli();;
//			sql="select count(1) as count from (select REPLACE(unix_timestamp(birth),'.','')/1000 as age,mbuser.* from mbuser where userid in(select DISTINCT userid from usergarage where modelid="+modelid+")) as t where t.age>"+age_40_50+" and t.age<"+age_30_40+"\r\n" +
//					""; 
//			age_40_50=NewMotoModelDAO.getCountByModelId(sql);
//			newMotoRankModel.put("age_40_50", age_40_50);
//
//			long age_50_up=LocalDateTime.of(LocalDate.now().plusYears(-50), LocalTime.now()).toInstant(ZoneOffset.of("+8")).toEpochMilli();;
//			sql="select count(1) as count from (select REPLACE(unix_timestamp(birth),'.','')/1000 as age,mbuser.* from mbuser where userid in(select DISTINCT userid from usergarage where modelid="+modelid+")) as t where t.age<"+age_50_up+"\r\n" +
//					""; 
//			age_50_up=NewMotoModelDAO.getCountByModelId(sql);
//			newMotoRankModel.put("age_50_up", age_50_up);
//
//			//地域
//			sql="select province,count(1) as count from mbuser where userid in (select DISTINCT(userid) from usergarage \r\n" + 
//					"where "
////					+ "addtime>="+starttime+" and addtime<"+endtime+" and "
//							+ "modelid="+modelid+") and LENGTH(province)>6 and province!=\"内蒙古\" GROUP BY province";
//			List<Map<String,Object>> diyulist=NewMotoModelDAO.selectList(sql);
//			newMotoRankModel.put("diyustr", JSON.toJSONString(diyulist));
			
			
		}
		result=JSON.parseArray(JSON.toJSONString(res), NewMotoRankModel.class);
		result.sort(new Comparator<NewMotoRankModel>() {

			@Override
			public int compare(NewMotoRankModel o1, NewMotoRankModel o2) {
				return Integer.parseInt(o2.hotcount-o1.hotcount+"");
			}
		});
		int c=0;
		for (NewMotoRankModel newMotoRankModel : result) {
			c++;
			newMotoRankModel.rankindex=c;
		}
		NewMotoModelDAO.insertRankModel(result);
		try {
			MotoCarRedisEsManager.getInstance().initRank(result);
		} catch (Exception e) {
			e.printStackTrace();
		};
	
	}
	
	private void getBoyGirlCountByBrandid(Map<String, Object> newMotoRankModel, int brandid) {
		Pipeline pipeline=RedisManager.getInstance().getPipeline(Consts.REDIS_SCHEME_USER);
		try {
			int boycount=0;
			int girlcount=0;
			int age_20_down=0;
			int age_20_30=0;
			int age_30_40=0;
			int age_40_50=0;
			int age_50_up=0;
			String sql="select DISTINCT userid from usergarage where brandid="+brandid;
			modelidOrBrandhandle(newMotoRankModel, pipeline, boycount, girlcount, age_20_down, age_20_30, age_30_40, age_40_50, age_50_up, sql);
		} finally {
			RedisManager.getInstance().close(pipeline);
		}
	}

	private void modelidOrBrandhandle(Map<String, Object> newMotoRankModel, Pipeline pipeline, int boycount, int girlcount, int age_20_down, int age_20_30, int age_30_40, int age_40_50, int age_50_up, String sql) {
		List<Map<String, Object>> userids=NewMotoModelDAO.selectList(sql);
		if(CollectionUtil.isNotEmpty(userids)) {
			List<List<Map<String, Object>>> newuserids=CollectionUtil.averageAssign(userids, userids.size()/5000);
			for (List<Map<String, Object>> map : newuserids) {
				
				for (Map<String, Object> useridsMap : map) {
					if(useridsMap.get("userid")!=null) {
						String userid=(String) useridsMap.get("userid");
						String key=userid+UserManager.USERKEY_USER;
						pipeline.hget(key, UserManager.MAPKEY_GENDER);
					}
					List<Object> result=pipeline.syncAndReturnAll();
					for (Object obj : result) {
						if(obj!=null) {
							String genderstr=(String) obj;
							if(genderstr.equals("0")) {
								boycount++;
							}else if(genderstr.equals("1")) {
								girlcount++;
							}
						}
					}
				}
				//处理省
				for (Map<String, Object> useridsMap : map) {
					if(useridsMap.get("userid")!=null) {
						String userid=(String) useridsMap.get("userid");
						String key=userid+UserManager.USERKEY_USER;
						pipeline.hget(key, UserManager.MAPKEY_PROVINCE);
					}
					List<Object> result=pipeline.syncAndReturnAll();
					for (Object obj : result) {
						if(obj!=null) {
							String genderstr=(String) obj;
							CityDataModel res=MotoDataManager.getInstance().getCityDataByProvinceName(genderstr);
							if(res!=null) {
								String pinfull=PinYinUtil.getFullSpell(res.province);
								if(res.province.equals("陕西省")) {
									pinfull+="_v2";
								}
								if(newMotoRankModel.containsKey(pinfull)) {
									int c=Integer.parseInt(newMotoRankModel.get(pinfull).toString());
									c++;
									newMotoRankModel.put(pinfull, c);
								}else {
									newMotoRankModel.put(pinfull, 1);
									}
								}
							}
					}
				}
				long age_20=LocalDateTime.of(LocalDate.now().plusYears(-20), LocalTime.now()).toInstant(ZoneOffset.of("+8")).toEpochMilli();;
//					newMotoRankModel.put("age_20_down", age_20_down);
				long age_30=LocalDateTime.of(LocalDate.now().plusYears(-30), LocalTime.now()).toInstant(ZoneOffset.of("+8")).toEpochMilli();;
//					newMotoRankModel.put("age_20_30", age_20_30);
				long age_40=LocalDateTime.of(LocalDate.now().plusYears(-40), LocalTime.now()).toInstant(ZoneOffset.of("+8")).toEpochMilli();;
//					newMotoRankModel.put("age_30_40", age_30_40);
				long age_50=LocalDateTime.of(LocalDate.now().plusYears(-50), LocalTime.now()).toInstant(ZoneOffset.of("+8")).toEpochMilli();;
//					newMotoRankModel.put("age_40_50", age_40_50);
//					long age_50_up=LocalDateTime.of(LocalDate.now().plusYears(-50), LocalTime.now()).toInstant(ZoneOffset.of("+8")).toEpochMilli();;
				//年龄分布
				for (Map<String, Object> useridsMap : map) {
					if(useridsMap.get("userid")!=null) {
						String userid=(String) useridsMap.get("userid");
						String key=userid+UserManager.USERKEY_USER;
						pipeline.hget(key, UserManager.MAPKEY_BIRTH);
					}
					List<Object> result=pipeline.syncAndReturnAll();
					for (Object obj : result) {
						if(obj!=null) {
							String bitrh=(String) obj;
							try {
								long age=DateUtil.date(bitrh).getTime();
								if(age>age_20) {
									age_20_down++;
								}else if(age>age_30&&age<age_20) {
									age_20_30++;
								}else if(age>age_40&&age<age_30) {
									age_30_40++;
								}else if(age>age_50&&age<age_40) {
									age_40_50++;
								}else if(age<age_50) {
									age_50_up++;
								}
							} catch (ParseException e) {
								e.printStackTrace();
							}
							}
					}
				}
			}
			newMotoRankModel.put("boycount", boycount);
			newMotoRankModel.put("girlcount", girlcount);
			newMotoRankModel.put("age_20_down", age_20_down);
			newMotoRankModel.put("age_20_30", age_20_30);
			newMotoRankModel.put("age_30_40", age_30_40);
			newMotoRankModel.put("age_40_50", age_40_50);
			newMotoRankModel.put("age_50_up", age_50_up);

			
//				newMotoRankModel.put("beijingshicount", beijingshicount);
		}
	}
	
	private void getBoyGirlCount(Map<String, Object> newMotoRankModel, int modelid) {
		Pipeline pipeline=RedisManager.getInstance().getPipeline(Consts.REDIS_SCHEME_USER);
		try {
			int boycount=0;
			int girlcount=0;
			int age_20_down=0;
			int age_20_30=0;
			int age_30_40=0;
			int age_40_50=0;
			int age_50_up=0;
			String sql="select DISTINCT userid from usergarage where modelid="+modelid;
			modelidOrBrandhandle(newMotoRankModel, pipeline, boycount, girlcount, age_20_down, age_20_30, age_30_40, age_40_50, age_50_up, sql);
		} finally {
			RedisManager.getInstance().close(pipeline);
		}

	

//		sql="select count(1) as count from mbuser where userid in(select userid from usergarage where modelid="+modelid+") and gender=0";
//		int boycount=NewMotoModelDAO.getCountByModelId(sql);
//		newMotoRankModel.put("boycount", boycount);
//		sql="select count(1) as count from mbuser where userid in(select userid from usergarage where modelid="+modelid+") and gender=1";
//		int girlcount=NewMotoModelDAO.getCountByModelId(sql);
//		newMotoRankModel.put("girlcount", girlcount);		
	}

	public static void main(String[] args) throws ParseException {
//		List s=Lists.newArrayList(0,1,3,2,1,2);
//		s.sort(new Comparator<Integer>() {
//
//			@Override
//			public int compare(Integer o1, Integer o2) {
//				// TODO Auto-generated method stub
//				return o2-o1;
//			}
//		});
//		
//		System.out.println(JSON.toJSONString(s));
		List<String> s=Lists.newArrayList("北京市",
				"天津市",
				"河北省",
				"山西省",
				"内蒙古自治区",
				"辽宁省",
				"吉林省",
				"黑龙江省",
				"上海市",
				"江苏省",
				"浙江省",
				"安徽省",
				"福建省",
				"江西省",
				"山东省",
				"河南省",
				"湖北省",
				"湖南省",
				"广东省",
				"广西壮族自治区",
				"海南省",
				"重庆市",
				"四川省",
				"贵州省",
				"云南省",
				"西藏自治区",
				"陕西省",
				"甘肃省",
				"青海省",
				"宁夏回族自治区",
				"新疆维吾尔自治区",
				"台湾省",
				"香港特别行政区",
				"澳门特别行政区",
				"外国");
		System.out.println(PinYinUtil.getFullSpell("内蒙古自治区"));
		for (String string : s) {
//			System.out.println("ALTER TABLE motomodel_new_rank ADD COLUMN  "+PinYinUtil.getFullSpell(string)+" bigint(20)  COMMENT '"+string+"';");
//			System.out.println("ALTER TABLE motomodel_new_v2 DROP  COLUMN  "+PinYinUtil.getFullSpell(string)+" ;");
			System.out.println("public long "+PinYinUtil.getFullSpell(string)+";//"+string+"");
//			System.out.println("#{item."+PinYinUtil.getFullSpell(string)+"},");
//			System.out.print(","+PinYinUtil.getFullSpell(string)+"=VALUES("+PinYinUtil.getFullSpell(string)+")");

		}
//		System.out.println(DateUtil.date("1986-04-28").getTime());;
//		System.out.println(LocalDateTime.of(LocalDate.now().plusYears(-20), LocalTime.now()).toInstant(ZoneOffset.of("+8")).toEpochMilli());
//		System.out.println(LocalDateTime.of(LocalDate.now().plusYears(-30), LocalTime.now()).toInstant(ZoneOffset.of("+8")).toEpochMilli());
	}
}
