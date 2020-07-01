package com.motobang.task.impl.newmotomodel;

import java.text.ParseException;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
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
import com.github.ltsopensource.tasktracker.runner.JobRunner;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.motoband.common.Consts;
import com.motoband.dao.newmotomodel.NewMotoModelDAO;
import com.motoband.manager.MotoDataManager;
import com.motoband.manager.RedisManager;
import com.motoband.manager.UserManager;
import com.motoband.manager.newmotomodel.MotoCarRedisEsManager;
import com.motoband.manager.search.ElasticSearchManager;
import com.motoband.model.CityDataModel;
import com.motoband.model.MotoBrandModelV2;
import com.motoband.model.MotoModelModel;
import com.motoband.model.MotoSeriesModel;
import com.motoband.model.NewMotoModel;
import com.motoband.model.NewMotoModelV2;
import com.motoband.model.NewMotoRankModel;
import com.motoband.utils.BeanUtils;
import com.motoband.utils.DateUtil;
import com.motoband.utils.ExecutorsUtils;
import com.motoband.utils.PinYinUtil;
import com.motoband.utils.RandomUtils;
import com.motoband.utils.collection.CollectionUtil;

import net.sf.cglib.core.Local;
import redis.clients.jedis.Pipeline;

public class NEWMOTOMODEL_RANK implements InterruptibleJobRunner  {
    protected static final Logger LOGGER = LoggerFactory.getLogger(NEWMOTOMODEL_RANK.class);

	@Override
	public Result run(JobContext jobContext) throws Throwable {
		try {
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
//			styleMap.put("新能源", 14);
//			styleMap.put("其它", 20);
//			LOGGER.error("NEWMOTOMODEL_RANK is start");	
//			LOGGER.debug("NEWMOTOMODEL_RANK is start");
			LOGGER.info("NEWMOTOMODEL_RANK is start,job="+com.github.ltsopensource.core.json.JSON.toJSONString(jobContext.getJob()));
//			LOGGER.info(LOGGER.isErrorEnabled());
//			LOGGER.info(LOGGER.isDebugEnabled());
//			LOGGER.info(LOGGER.isInfoEnabled());
			int year=LocalDate.now().getYear();
			String yearstr=jobContext.getJob().getParam("year");
			if(StringUtils.isNotEmpty(yearstr)){
				year=Integer.parseInt(yearstr);
			}
			LOGGER.info("year="+year);
			int mongth=LocalDate.now().getMonthValue();
			String monthstr=jobContext.getJob().getParam("month");
			if(StringUtils.isNotEmpty(monthstr)){
				mongth=Integer.parseInt(monthstr);
			}else {
				mongth++;
			}
			LOGGER.info("mongth="+mongth);
				handleModelid(year,mongth,styleMap);
				LOGGER.info("NEWMOTOMODEL_RANK is handleBrandid start");	
				handleBrandid(year,mongth,styleMap);
		} catch (Exception e) {
        		LOGGER.error("ERROR="+ExceptionUtils.getStackTrace(e));
                return new Result(Action.EXECUTE_FAILED, ExceptionUtils.getStackTrace(e));
		}
		return null;
	}

	



	private void handleModelid(Integer year,Integer month,Map<String, Integer> styleMap) {
		LocalDateTime now=LocalDateTime.of(year, month, 1,0,0,0);
		long starttime=now.plusMonths(-1).toInstant(ZoneOffset.of("+8")).toEpochMilli();
		long endtime=now.toInstant(ZoneOffset.of("+8")).toEpochMilli();
		LOGGER.info("現在開始統計車型,"+now.getYear()+"年"+now.plusMonths(-1).getMonthValue()+"月"+",到"+DateUtil.getDateTimeOfTimestamp(starttime).getYear()+"年"+now.getMonthValue()+"月");

		//查询时间段内的线路
		String sql="select modelid from motoseries";
//		String sql="select modelid,SUM(mileage) as mileage ,AVG(maxspeed) avgmaxspeed,AVG(avgspeed) avgspeed from rideline \r\n" + 
//				"where reporttime>="+starttime+" and reporttime<"+endtime+" GROUP BY modelid ";
//		String sql="select modelid,SUM(mileage) as mileage ,AVG(maxspeed) avgmaxspeed,AVG(avgspeed) avgspeed from rideline \r\n" + 
//" where reporttime>=1585670400000 and reporttime<1585699200000 GROUP BY modelid";
		List<Map<String, Object>> res=NewMotoModelDAO.selectList(sql);
		LOGGER.info("handleModelid 縂共需處理count="+res.size());
		AtomicInteger indexcount=new AtomicInteger();
		List<List<Map<String, Object>>> listRes=CollectionUtil.averageAssign(res, res.size()/20+1);
		List<NewMotoRankModel> result=Lists.newCopyOnWriteArrayList();
		CyclicBarrier cb=new CyclicBarrier(listRes.size()+1);
		for (List<Map<String, Object>> list : listRes) {
			ExecutorsUtils.getInstance().execute(new Runnable() {
				@Override
				public void run() {
					try {
						for (Map<String, Object> newMotoRankModel : list) {
							int modelid=Integer.parseInt(newMotoRankModel.get("modelid")+"");
							LOGGER.info("modelid="+modelid+",indexcount="+indexcount+",ranktime="+starttime);

//							List<NewMotoModelV2> newMotoModelV2=MotoDataManager.getInstance().getNewMotoModelListByModelidV2(modelid);
//							if(CollectionUtil.isEmpty(newMotoModelV2)){
////								res.remove(newMotoModelV2);
//								continue;
//							}
							newMotoRankModel.put("brandid", null);
							String sql="select modelid,SUM(mileage) as mileage ,AVG(maxspeed) avgmaxspeed,AVG(avgspeed) avgspeed from rideline where modelid ="+modelid +" and reporttime>="+starttime+" and reporttime<"+endtime;;
							 List<Map<String, Object>> mileageAndMaxspeedAndAvgSpeedMap=NewMotoModelDAO.selectList(sql);
							 Long mileage=0l;
							 if(CollectionUtil.isNotEmpty(mileageAndMaxspeedAndAvgSpeedMap)) {
								 Map<String, Object> map=mileageAndMaxspeedAndAvgSpeedMap.get(0);
								 if(map!=null) {
									 newMotoRankModel.putAll(map);
									 if(newMotoRankModel.containsKey("mileage")){
										 mileage=Long.parseLong(newMotoRankModel.get("mileage").toString());
										 mileage=mileage*3+RandomUtils.randomNumber(1, 3);
										 newMotoRankModel.put("mileage", mileage);
									 }
								 }
							 }

//							 newMotoRankModel.putAll(m);
//							 newMotoRankModel.put("mileage", value)
							indexcount.incrementAndGet();
							newMotoRankModel.put("ranktime", starttime);
//							long hotcount =0;
//							try {
//								MotoSeriesModel itemModel=BeanUtils.mapToObject(RedisManager.getInstance().hgetAll(Consts.REDIS_SCHEME_RUN, modelid+MotoCarRedisEsManager.RUNKEY_MOTOSERIESV2INFO), MotoSeriesModel.class);
//								if(itemModel!=null) {
//									hotcount=itemModel.serieshotcount*1000;;
//								}
//								if(hotcount==0){
//									hotcount=RandomUtils.randomNumber(1, 10)*1000;
////									RedisManager.getInstance().hset(Consts.REDIS_SCHEME_RUN, modelid+MotoCarRedisEsManager.RUNKEY_MOTOSERIESV2INFO,"serieshotcount",hotcount+"");
//								}
//								
//							} catch (Exception e) {
//								e.printStackTrace();
//							}
					
//							newMotoRankModel.put("hotcount",hotcount);
							
							long prevmonthstarttime=now.plusMonths(-2).toInstant(ZoneOffset.of("+8")).toEpochMilli();
//							sql="select totalhotcount as count from motomodel_new_rank where modelid="+modelid+" and  ranktime="+prevmonthstarttime+" and ranktype=0";
//							long count=NewMotoModelDAO.getCountByModelId(sql);
//							newMotoRankModel.put("totalhotcount", count+hotcount);
							long totalmileage=0l;
							long totalreadcount=0l;
							sql="select totalmileage as count from motomodel_new_rank where modelid="+modelid+" and ranktime="+prevmonthstarttime+" and ranktype=0";
							long count=NewMotoModelDAO.getCountByModelId(sql);
							if(newMotoRankModel.containsKey("mileage")) {
//									mileage=Long.parseLong(newMotoRankModel.get("mileage").toString());
								totalmileage=count+mileage;
								newMotoRankModel.put("totalmileage", count+mileage);
							}else {
								totalmileage=count;
								newMotoRankModel.put("totalmileage", count);
							}
							
							
							sql="select count(1) as count from usergarage where modelid="+modelid+" and addtime>="+starttime+" and addtime<="+endtime+"\r\n" + 
									"";
							long usercount=NewMotoModelDAO.getCountByModelId(sql);
							newMotoRankModel.put("usercount", usercount);
							sql="select count(DISTINCT userid) as count from usergarage where modelid="+modelid+" and addtime<="+endtime;
							long totalusercount=NewMotoModelDAO.getCountByModelId(sql);
								 totalusercount=totalusercount*3+RandomUtils.randomNumber(1, 3);
							newMotoRankModel.put("totalusercount", totalusercount);
							
							
							String seriesreadcountstr=RedisManager.getInstance().hget(Consts.REDIS_SCHEME_RUN, modelid+MotoCarRedisEsManager.RUNKEY_MOTOSERIESV2INFO, MotoCarRedisEsManager.MAPKEY_SERIES_READCOUNT);
							if(StringUtils.isBlank(seriesreadcountstr)) {
								seriesreadcountstr="0";
							}
							long brandreadcount=Long.valueOf(seriesreadcountstr);
//							long totalhotcount=brandreadcount+totalmileage+totalusercount*1000;
							long totalhotcount=brandreadcount*30;
							totalreadcount=brandreadcount;
							LOGGER.info("modelid="+modelid+",totalhotcount="+totalhotcount+",ranktime="+starttime);
							newMotoRankModel.put("totalhotcount",totalhotcount);
							newMotoRankModel.put("totalreadcount",totalreadcount);
							//当月的totalreadcount-上月个totalreadcount
							long prevranktime= now.plusMonths(-2).toInstant(ZoneOffset.of("+8")).toEpochMilli();
							sql="select totalreadcount as count from motomodel_new_rank where modelid="+modelid+" and ranktime="+prevranktime;
							long prevtotalreadcount=NewMotoModelDAO.getCountByModelId(sql);
							long readcount=Math.abs(totalreadcount-prevtotalreadcount);
							newMotoRankModel.put("readcount",readcount);
//							long hotcount=readcount+mileage+usercount*1000;
							long hotcount=readcount*30;
							LOGGER.info("modelid="+modelid+",hotcount="+hotcount+",ranktime="+starttime);
							newMotoRankModel.put("hotcount",hotcount);
							
							MotoModelModel motomodel=MotoDataManager.getInstance().getMotoModel(modelid);
							String style="";
							if(motomodel!=null){
								if(motomodel!=null){
									 style+=styleMap.get(motomodel.style);
								}
							}else{
								List<NewMotoModelV2> newmotomodel=MotoDataManager.getInstance().getNewMotoModelListByModelidV2(modelid);
								if(CollectionUtil.isNotEmpty(newmotomodel)) {
									for (NewMotoModelV2 newMotoRankModel2 : newmotomodel) {
										 Integer tempstyle=newMotoRankModel2.style;
										 if(tempstyle!=null){
											 style+=tempstyle+",";
										 }
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

//							if(endtime==1577808000000l) {
//								sql="select count(DISTINCT userid) as count from usergarage where modelid="+modelid+" and addtime<=1577808000000";
//								long totalusercount=NewMotoModelDAO.getCountByModelId(sql);
//								newMotoRankModel.put("totalusercount", totalusercount);
//							}else {
//								sql="select totalusercount as count from motomodel_new_rank where modelid="+modelid+" and ranktime="+prevmonthstarttime+" and ranktype=0";
//								long totalusercount=NewMotoModelDAO.getCountByModelId(sql);
				//
//								newMotoRankModel.put("totalusercount", count+totalusercount);
//							}
							
							
							sql="select DISTINCT(makertype) as makertype from motomodel_new_v2 where modelid="+modelid;
							List<Map<String, Object>> makertypeList=NewMotoModelDAO.selectList(sql);
							String makertypeStr="";
							List<String> makert=Lists.newArrayList();
							if(CollectionUtil.isNotEmpty(makertypeList)) {
								for (Map makertypeMap : makertypeList) {
									if(makertypeMap!=null&&makertypeMap.containsKey("makertype")) {
										if(makertypeMap.get("makertype")!=null) {
											makert.add(makertypeMap.get("makertype").toString());
										}
//										makertypeStr+=makertypeMap.get("makertype")+",";
									}
								}
							}
							if(CollectionUtil.isNotEmpty(makert)) {
//								if(makertypeStr.charAt(makertypeStr.length()-1)==',') {
//									makertypeStr=makertypeStr.substring(0,makertypeStr.length()-1);
//								}
								makertypeStr=CollectionUtil.join(makert, ",");
								newMotoRankModel.put("makertype", makertypeStr);
							}
							newMotoRankModel.put("ranktype", 0);
							newMotoRankModel.put("brandid",null);
							//男女比例
							getBoyGirlCount(newMotoRankModel,modelid);
							result.add(JSON.parseObject(JSON.toJSONString(newMotoRankModel),NewMotoRankModel.class));

						}
					} finally {
						try {
							cb.await();
						} catch (InterruptedException | BrokenBarrierException e) {
							e.printStackTrace();
						}
					}

				}
			});
		}

//		result=JSON.parseArray(JSON.toJSONString(res), NewMotoRankModel.class);
		//排序时应对无论当月有没有骑行的车型都要排序 因为如果当月没有骑行它的排名还是旧的。
		try {
			cb.await();
		} catch (InterruptedException | BrokenBarrierException e1) {
			e1.printStackTrace();
		}
		sql="";
		result.sort(new Comparator<NewMotoRankModel>() {

			@Override
			public int compare(NewMotoRankModel o1, NewMotoRankModel o2) {
//				return Integer.parseInt(o2.hotcount-o1.hotcount+"");
				if(o1.modelid.equals("0")) {
					return 1;
				}
				if(o2.modelid.equals("0")) {
					return -1;
				}
				if(o1.hotcount==o2.hotcount) {
					return 0;
				}else {
					return o1.hotcount > o2.hotcount ? -1 :1;
				}
			}
		});
		int c=0;
		for (NewMotoRankModel newMotoRankModel : result) {
			c++;
			if(newMotoRankModel.modelid.equals("0")) {
				newMotoRankModel.rankindex=9999;
			}else {
				newMotoRankModel.rankindex=c;
			}
			newMotoRankModel.updatetime=System.currentTimeMillis();

		}
		NewMotoModelDAO.insertRankModel(result);
		try {
//			if (month==LocalDateTime.now().getMonthValue()&&year==LocalDateTime.now().getYear()) {
				MotoCarRedisEsManager.getInstance().initSeriesRank(result);
				for (NewMotoRankModel newMotoRankModel : result) {
					Map<String,Object> map=Maps.newHashMap();
					map.put("modelid", newMotoRankModel.modelid);
					map.put("serieshotcount", newMotoRankModel.hotcount);
					ElasticSearchManager.getInstance().syncUpdateByQuery(NewMotoModelV2.class,JSON.toJSONString(map));
				}
				LOGGER.info("同步当月车型排行榜缓存,month="+month);
//			}else {
//				LOGGER.info("不同步当月品牌排行榜缓存,month="+month);
//
//			}
		} catch (Exception e) {
			e.printStackTrace();
		};
	
	}
	
	private void handleBrandid(Integer year,Integer month,Map<String, Integer> styleMap) {
		LocalDateTime now=LocalDateTime.of(year, month, 1,0,0,0);
		long starttime=now.plusMonths(-1).toInstant(ZoneOffset.of("+8")).toEpochMilli();
		long endtime=now.toInstant(ZoneOffset.of("+8")).toEpochMilli();
		LOGGER.info("現在開始統計品牌,"+now.getYear()+"年"+now.plusMonths(-1).getMonthValue()+"月"+",到"+DateUtil.getDateTimeOfTimestamp(starttime).getYear()+"年"+now.getMonthValue()+"月");
		//查询时间段内的线路
//		String sql="select brandid,SUM(mileage) as mileage ,AVG(maxspeed) avgmaxspeed,AVG(avgspeed) avgspeed from rideline \r\n" + 
//				"where reporttime>="+starttime+" and reporttime<"+endtime+" GROUP BY brandid";
		String sql ="select brandid from motobrand_v2";
//		String sql="select brandid,SUM(mileage) as mileage ,AVG(maxspeed) avgmaxspeed,AVG(avgspeed) avgspeed from rideline \r\n" + 
//" where reporttime>=1585670400000 and reporttime<1585699200000 GROUP BY brandid";
//		RedisManager.getInstance().hget(Consts.REDIS_SCHEME_NEWS, key, field)
		List<Map<String,Object>> res=NewMotoModelDAO.selectList(sql);
		LOGGER.info("handleBrandid 縂共需處理count="+res.size());
		List<List<Map<String, Object>>> listRes=CollectionUtil.averageAssign(res, res.size()/20+1);
		List<NewMotoRankModel> result=Lists.newCopyOnWriteArrayList();
		CyclicBarrier cb=new CyclicBarrier(listRes.size()+1);
		AtomicInteger indexcount=new AtomicInteger();
		for (List<Map<String, Object>> list : listRes) {
			ExecutorsUtils.getInstance().execute(new Runnable() {
				@Override
				public void run() {
					try {
						for (Map<String,Object> newMotoRankModel : list) {
							int brandid=Integer.parseInt(newMotoRankModel.get("brandid")+"");
							indexcount.incrementAndGet();
							newMotoRankModel.put("ranktime", starttime);
							LOGGER.info("brandid="+brandid+",indexcount="+indexcount+",ranktime="+starttime);

							String sql="select SUM(mileage) as mileage ,AVG(maxspeed) avgmaxspeed,AVG(avgspeed) avgspeed from rideline where brandid="+brandid+" and reporttime>="+starttime+" and reporttime<"+endtime;
							 List<Map<String, Object>> mileageAndMaxspeedAndAvgSpeedMap=NewMotoModelDAO.selectList(sql);
							 Long mileage=0l;
							 if(CollectionUtil.isNotEmpty(mileageAndMaxspeedAndAvgSpeedMap)) {
								 Map<String, Object> map=mileageAndMaxspeedAndAvgSpeedMap.get(0);
								 if(map!=null) {
									 newMotoRankModel.putAll(map);
									 if(newMotoRankModel.containsKey("mileage")){
										 mileage=Long.parseLong(newMotoRankModel.get("mileage").toString());
										 mileage=mileage*3+RandomUtils.randomNumber(1, 3);
										 newMotoRankModel.put("mileage", mileage);
									 }
								 }
							 }
							long totalhotcount = 0;
							long totalreadcount=0;
							long prevmonthstarttime=now.plusMonths(-2).toInstant(ZoneOffset.of("+8")).toEpochMilli();
//							sql="select totalhotcount as count from motomodel_new_rank where brandid="+brandid+" and ranktime="+prevmonthstarttime+" and ranktype=1";
//							long count=NewMotoModelDAO.getCountByModelId(sql);
//							newMotoRankModel.put("totalhotcount", count+hotcount);
							long totalmileage=0;
							sql="select totalmileage as count from motomodel_new_rank where brandid="+brandid+" and ranktime="+prevmonthstarttime+" and ranktype=1";
							long count=NewMotoModelDAO.getCountByModelId(sql);
							if(newMotoRankModel.containsKey("mileage")) {
//								long mileage=Long.parseLong(newMotoRankModel.get("mileage").toString());
								totalmileage=count+mileage;
								newMotoRankModel.put("totalmileage",totalmileage);
							}else {
								totalmileage=count;
								newMotoRankModel.put("totalmileage", totalmileage);
							}
//							newMotoRankModel.put("totalmileage", count+mileage);
							
							newMotoRankModel.put("ranktype", 1);
							newMotoRankModel.put("ranktime", starttime);
//							newMotoRankModel.put("rankid", MD5.stringToMD5(newMotoRankModel.get("brandid")+"-"+endtime));;
							sql="select count(1) as count from usergarage where brandid="+brandid+" and addtime>="+starttime+" and addtime<="+endtime+"\r\n" + 
									"";
							long usercount=NewMotoModelDAO.getCountByModelId(sql);
							usercount=usercount*3+RandomUtils.randomNumber(1, 3);
							newMotoRankModel.put("usercount", usercount);
							sql="select count(DISTINCT userid) as count from usergarage where brandid="+brandid+" and addtime<="+endtime;
							long totalusercount=NewMotoModelDAO.getCountByModelId(sql);
							totalusercount=totalusercount*3+RandomUtils.randomNumber(1, 3);
							newMotoRankModel.put("totalusercount", totalusercount);
							String brandreadcountstr=RedisManager.getInstance().hget(Consts.REDIS_SCHEME_RUN, brandid+MotoCarRedisEsManager.RUNKEY_MOTOBRANDV2INFO, MotoCarRedisEsManager.MAPKEY_BRAND_READCOUNT);
							if(StringUtils.isBlank(brandreadcountstr)) {
								brandreadcountstr="0";
							}
							long brandreadcount=Long.valueOf(brandreadcountstr);
//							totalhotcount=brandreadcount+totalmileage+totalusercount*1000;
							totalhotcount=brandreadcount*30;
							totalreadcount=brandreadcount;
							LOGGER.info("brandid="+brandid+",totalhotcount="+totalhotcount+",ranktime="+starttime);

							newMotoRankModel.put("totalhotcount",totalhotcount);
							newMotoRankModel.put("totalreadcount",totalreadcount);
							//当月的totalreadcount-上月个totalreadcount
							long prevranktime= now.plusMonths(-2).toInstant(ZoneOffset.of("+8")).toEpochMilli();
							sql="select totalreadcount as count from motomodel_new_rank where brandid="+brandid+" and ranktime="+prevranktime;
							long prevtotalreadcount=NewMotoModelDAO.getCountByModelId(sql);
							long readcount=totalreadcount-prevtotalreadcount;
							newMotoRankModel.put("readcount",readcount);
//							long hotcount=readcount+mileage+usercount*1000;
							long hotcount=readcount*30;
							LOGGER.info("brandid="+brandid+",hotcount="+hotcount+",ranktime="+starttime);
							newMotoRankModel.put("hotcount",hotcount);
//							if(endtime==1577808000000l) {
//								sql="select count(DISTINCT userid) as count from usergarage where brandid="+brandid+" and addtime<=1577808000000";
//								long totalusercount=NewMotoModelDAO.getCountByModelId(sql);
//								newMotoRankModel.put("totalusercount", totalusercount);
//							}else {
//								sql="select totalusercount as count from motomodel_new_rank where brandid="+brandid+" and ranktime="+prevmonthstarttime+" and ranktype=1";
//								count=NewMotoModelDAO.getCountByModelId(sql);
//								newMotoRankModel.put("totalusercount", count+usercount);
//							}
					
							
							
							sql="select DISTINCT(makertype) as makertype from motomodel_new_v2 where brandid="+brandid;
							List<Map<String, Object>> makertypeList=NewMotoModelDAO.selectList(sql);
							String makertypeStr="";
							List<String> makert=Lists.newArrayList();
							if(CollectionUtil.isNotEmpty(makertypeList)) {
								for (Map makertypeMap : makertypeList) {
									if(makertypeMap!=null&&makertypeMap.containsKey("makertype")) {
										if(makertypeMap.get("makertype")!=null) {
											makert.add(makertypeMap.get("makertype").toString());
										}
//										makertypeStr+=makertypeMap.get("makertype")+",";
									}
								}
							}
							if(CollectionUtil.isNotEmpty(makert)) {
//								if(makertypeStr.charAt(makertypeStr.length()-1)==',') {
//									makertypeStr=makertypeStr.substring(0,makertypeStr.length()-1);
//								}
								makertypeStr=CollectionUtil.join(makert, ",");
								newMotoRankModel.put("makertype", makertypeStr);
							}
							newMotoRankModel.put("modelid", null);
							//男女比例
							getBoyGirlCountByBrandid(newMotoRankModel,brandid);
							result.add(JSON.parseObject(JSON.toJSONString(newMotoRankModel),NewMotoRankModel.class));

						}
					} finally {
						try {
							cb.await();
						} catch (InterruptedException | BrokenBarrierException e) {
							e.printStackTrace();
						}
					}
				}
			});
		}
//		List<NewMotoRankModel> result=Lists.newArrayList();

//		result=JSON.parseArray(JSON.toJSONString(res), NewMotoRankModel.class);
		try {
			cb.await();
		} catch (InterruptedException | BrokenBarrierException e1) {
			e1.printStackTrace();
		}
		//排序时应对无论当月有没有骑行的车型都要排序
		result.sort(new Comparator<NewMotoRankModel>() {

			@Override
			public int compare(NewMotoRankModel o1, NewMotoRankModel o2) {
//				return Integer.parseInt((o2.hotcount-o1.hotcount)+"");
				if(o1.brandid.equals("0")) {
					return 1;
				}
				if(o2.brandid.equals("0")) {
					return -1;
				}
				if(o1.hotcount==o2.hotcount) {
					return 0;
				}else {
					return o1.hotcount > o2.hotcount ? -1 :1;
				}
			}
		});
		int c=0;
		for (NewMotoRankModel newMotoRankModel : result) {
			c++;
			if(newMotoRankModel.brandid.equals("93")) {
				newMotoRankModel.rankindex=999;
			}else {
				newMotoRankModel.rankindex=c;
			}
			newMotoRankModel.updatetime=System.currentTimeMillis();
		}
		NewMotoModelDAO.insertRankModel(result);	
		try {
//			if (month==LocalDateTime.now().getMonthValue()&&year==LocalDateTime.now().getYear()) {
				MotoCarRedisEsManager.getInstance().initBrandRank(result);
				MotoCarRedisEsManager.getInstance().initMotoBrandsV2();
				//同步brandhotcount;
				for (NewMotoRankModel newMotoRankModel : result) {
					Map<String,Object> map=Maps.newHashMap();
					map.put("brandid", newMotoRankModel.brandid);
					map.put("brandhotcount", newMotoRankModel.hotcount);
					ElasticSearchManager.getInstance().syncUpdateByQuery(NewMotoModelV2.class,JSON.toJSONString(map));
				}
				LOGGER.info("同步当月品牌排行榜缓存,month="+month);
//			}else {
//				LOGGER.info("不同步当月品牌排行榜缓存,month="+month);
//			}
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
        DateTimeFormatter pattern = DateTimeFormatter.ofPattern("yyyy-MM-dd");
        DateTimeFormatter pattern2 = DateTimeFormatter.ofPattern("yyyy/MM/dd");


		List<Map<String, Object>> userids=NewMotoModelDAO.selectList(sql);
		if(CollectionUtil.isNotEmpty(userids)) {
			List<List<Map<String, Object>>> newuserids=CollectionUtil.averageAssign(userids, userids.size()/10000+1);
			for (List<Map<String, Object>> map : newuserids) {
				
				for (Map<String, Object> useridsMap : map) {
					if(useridsMap.get("userid")!=null) {
						String userid=(String) useridsMap.get("userid");
						String key=userid+UserManager.USERKEY_USER;
						pipeline.hget(key, UserManager.MAPKEY_GENDER);
					}
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
				//处理省
				for (Map<String, Object> useridsMap : map) {
					if(useridsMap.get("userid")!=null) {
						String userid=(String) useridsMap.get("userid");
						String key=userid+UserManager.USERKEY_USER;
						pipeline.hget(key, UserManager.MAPKEY_PROVINCE);
					}
				}
				result=pipeline.syncAndReturnAll();
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
					
				}
				result=pipeline.syncAndReturnAll();
				for (Object obj : result) {
					if(obj!=null) {
						try {
							String bitrh=(String) obj;
							if(StringUtils.isEmpty(bitrh)){
								continue;
							}
							bitrh=bitrh.trim();
//								LocalDateTime localDateTime = LocalDateTime.parse(bitrh, timeDtf).toInstant(ZoneOffset.of("+8")).toEpochMilli();;
//								ZoneId zone = ZoneId.systemDefault();
//								Instant instant = localDateTime.atZone(zone).toInstant()
//								long age=DateUtil.date(bitrh).getTime();
//					        LocalDate localDate = LocalDate.parse(bitrh);
					        long age=0;
//					        System.out.println(  LocalDateTime.of(LocalDate.parse("1981-12-11", pattern), LocalTime.now()).toInstant(ZoneOffset.of("+8")).toEpochMilli());
							if(bitrh.indexOf("-")!=-1) {
//								System.out.println(bitrh.equals("1981-12-11"));
//						        System.out.println( LocalDateTime.of(LocalDate.parse(bitrh, pattern), LocalTime.now()).toInstant(ZoneOffset.of("+8")).toEpochMilli());

								age=LocalDateTime.of(LocalDate.parse(bitrh, pattern), LocalTime.now()).toInstant(ZoneOffset.of("+8")).toEpochMilli();
							}else if(bitrh.indexOf("/")!=-1) {
//								age=LocalDateTime.of(LocalDate.parse(bitrh, pattern2), LocalTime.now()).toInstant(ZoneOffset.of("+8")).toEpochMilli();
								
							}
//							long age=LocalDateTime.parse(bitrh, timeDtf).toInstant(ZoneOffset.of("+8")).toEpochMilli();
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
						} catch (Exception e) {
							continue;
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
		List<Integer> list=Lists.newArrayList(1,10,2,5,1,2,3);
		list.sort(new Comparator<Integer>() {

			@Override
			public int compare(Integer o1, Integer o2) {
				System.out.println(JSON.toJSONString(list));
				System.out.println("o1="+o1);
				System.out.println("o2="+o2);
				if(o1==10) {
					return 1;
				}
				if(o2==10) {
					return -1;
				}
				if(o1==o2) {
					return 0;
				}else {
					return o1 > o2 ? -1 :1;
				}
			}
		});
		System.out.println(JSON.toJSONString(list));
//        LocalDate localDate = LocalDate.parse("2019-12-07");
//        DateTimeFormatter pattern = DateTimeFormatter.ofPattern("yyyy/MM/dd");
//        	localDate = LocalDate.parse("2019-12-07",pattern);
//        LocalDateTime.of(LocalDate.parse("2019-12-07", pattern), LocalTime.now()).toInstant(ZoneOffset.of("+8")).toEpochMilli();
//        long endSecond = LocalDateTime.parse("2019-12-07 ",DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")).toEpochSecond(ZoneOffset.of("+8"));
//        System.out.println(endSecond);
//        LocalDateTime.of(LocalDate.parse("2019-12-07", pattern), time)
//        System.out.println(LocalDate.parse("2017/7/9", pattern));
//        System.out.println(  LocalDateTime.of(LocalDate.parse("2017/7/9", pattern), LocalTime.now()).toInstant(ZoneOffset.of("+8")).toEpochMilli());



//		 ZonedDateTime zdt = ZonedDateTime.parse("1981-12-11");
//	        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd");
//	        System.out.println(formatter.format(zdt));
//
//	        DateTimeFormatter zhFormatter = DateTimeFormatter.ofPattern("yyyy MMM dd EE HH:mm", Locale.CHINA);
//	        System.out.println(zhFormatter.format(zdt));
//
//	        DateTimeFormatter usFormatter = DateTimeFormatter.ofPattern("E, MMMM/dd/yyyy HH:mm", Locale.US);
//	        System.out.println(usFormatter.format(zdt));
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
//		LOGGER.info(JSON.toJSONString(s));
//		List<String> s=Lists.newArrayList("北京市",
//				"天津市",
//				"河北省",
//				"山西省",
//				"内蒙古自治区",
//				"辽宁省",
//				"吉林省",
//				"黑龙江省",
//				"上海市",
//				"江苏省",
//				"浙江省",
//				"安徽省",
//				"福建省",
//				"江西省",
//				"山东省",
//				"河南省",
//				"湖北省",
//				"湖南省",
//				"广东省",
//				"广西壮族自治区",
//				"海南省",
//				"重庆市",
//				"四川省",
//				"贵州省",
//				"云南省",
//				"西藏自治区",
//				"陕西省",
//				"甘肃省",
//				"青海省",
//				"宁夏回族自治区",
//				"新疆维吾尔自治区",
//				"台湾省",
//				"香港特别行政区",
//				"澳门特别行政区",
//				"外国");
//		LOGGER.info(PinYinUtil.getFullSpell("内蒙古自治区"));
//		for (String string : s) {
//			LOGGER.info("ALTER TABLE motomodel_new_rank ADD COLUMN  "+PinYinUtil.getFullSpell(string)+" bigint(20)  COMMENT '"+string+"';");
//			LOGGER.info("ALTER TABLE motomodel_new_v2 DROP  COLUMN  "+PinYinUtil.getFullSpell(string)+" ;");
//			LOGGER.info("public long "+PinYinUtil.getFullSpell(string)+";//"+string+"");
//			LOGGER.info("#{item."+PinYinUtil.getFullSpell(string)+"},");
//			System.out.print(","+PinYinUtil.getFullSpell(string)+"=VALUES("+PinYinUtil.getFullSpell(string)+")");

//		}
//		LOGGER.info(DateUtil.date("1986-04-28").getTime());;
//		LOGGER.info(LocalDateTime.of(LocalDate.now().plusYears(-20), LocalTime.now()).toInstant(ZoneOffset.of("+8")).toEpochMilli());
//		LOGGER.info(LocalDateTime.of(LocalDate.now().plusYears(-30), LocalTime.now()).toInstant(ZoneOffset.of("+8")).toEpochMilli());
	}





	@Override
	public void interrupt() {
		LOGGER.error("中断");
	}
}
