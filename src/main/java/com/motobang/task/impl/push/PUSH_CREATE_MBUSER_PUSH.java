package com.motobang.task.impl.push;

import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.commons.lang3.exception.ExceptionUtils;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.github.ltsopensource.core.domain.Action;
import com.github.ltsopensource.core.logger.Logger;
import com.github.ltsopensource.core.logger.LoggerFactory;
import com.github.ltsopensource.tasktracker.Result;
import com.github.ltsopensource.tasktracker.runner.InterruptibleJobRunner;
import com.github.ltsopensource.tasktracker.runner.JobContext;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.motoband.dao.UserDAO;
import com.motoband.factory.TIMFactory;
import com.motoband.manager.MotoDataManager;
import com.motoband.model.CityDataModel;
import com.motoband.model.GarageModel;
import com.motoband.model.MotoBrandModel;
import com.motoband.model.task.MBUserPushModel;
import com.motoband.utils.ExecutorsUtils;
import com.motoband.utils.OkHttpClientUtil;
import com.motoband.utils.collection.CollectionUtil;

import okhttp3.Response;

/**
 * 筛选有效用户 创建一个新的用户表，同步到ES 目前支持的字段 userid,province,city,gender, model,brand
 * ,addtime,lastactivetime,ctype Created by junfei.Yang on 2020年3月12日.
 */
public class PUSH_CREATE_MBUSER_PUSH implements InterruptibleJobRunner {
	protected static final Logger LOGGER = LoggerFactory.getLogger(PUSH_CREATE_MBUSER_PUSH.class);

	public static void main(String[] args) {
		StringBuffer sb = new StringBuffer();
		for (int i = 0; i < 12; i++) {
			sb.append("alter table userloginonlog");
			String d = LocalDate.now().plusMonths(-i).format(DateTimeFormatter.ofPattern("_yyyy_M"));
			sb.append(d);
			sb.append(" add lonlat varchar(255),");
			sb.append(" add citycode varchar(255);");
			sb.append("\r\n");
		}
		System.out.println(sb.toString());

//		System.out.println(LocalDateTime.of(LocalDate.now().plusYears(-2), LocalTime.now()).toInstant(ZoneOffset.of("+8")).toEpochMilli());
	}

	@Override
	public Result run(JobContext arg0) throws Throwable {
		AtomicInteger isvalidusercount=new AtomicInteger();
		try {
			LOGGER.info("更新用戶有效性 is start");
//			long minaddtime = LocalDateTime.of(LocalDate.now().plusYears(-2), LocalTime.now()).toInstant(ZoneOffset.of("+8")).toEpochMilli();
			int start=0;
			int pagesize=10000;
//			StringBuffer sql = new StringBuffer("select userid,city,gender,addtime from mbuser where  isro=0");
			while (true) {
				StringBuffer sql = new StringBuffer("select userid,city,gender,addtime from mbuser where  isro=0");
				sql.append(" limit "+start+","+pagesize);
				LOGGER.info("更新用戶有效性 start="+start+",pagesize="+pagesize);
				List<Map<String, Object>> result = UserDAO.executesql(sql.toString());
//				List<String> mbusermodeljsonstr = Lists.newArrayList();
//				List<String> userids = Lists.newArrayList();
				List<List<Map<String, Object>>> averageList=CollectionUtil.averageAssign(result, 5);
				CyclicBarrier cb=new CyclicBarrier(averageList.size()+1);
				for (List<Map<String, Object>> list : averageList) {
					ExecutorsUtils.getInstance().execute(new Runnable() {
						@Override
						public void run() {
							try {
								List<MBUserPushModel> pushlist=Lists.newArrayList();
								for (Map<String, Object> map : list) {
									String userid = (String) map.get("userid");
									LOGGER.info("userid="+userid+",开始检测");
									MBUserPushModel mbuser = new MBUserPushModel();
									mbuser.userid= userid;
									if (map.containsKey("city")) {
										String city = (String) map.get("city");
										// 获取北京的citydatamodel
										CityDataModel citydata = MotoDataManager.getInstance().getOldCityName(city);
										if (citydata != null) {
											citydata = MotoDataManager.getInstance().getCityData(citydata.citycode);
											mbuser.province = citydata.province;
											mbuser.city = citydata.name;
										} else {
											citydata = MotoDataManager.getInstance().getCityData(city);
											if (citydata != null) {
												mbuser.province = citydata.province;
												mbuser.city = citydata.name;
											}
										}
									}
									if (map.containsKey("gender")) {
										mbuser.gender = (int) map.get("gender");
									}
									String selectGrage = "select * from usergarage where userid=\"" + userid + "\" and `use`=1";
									List<Map<String, Object>> grageList = UserDAO.executesql(selectGrage);
									for (Map<String, Object> garageMap : grageList) {
										GarageModel g = JSON.parseObject(JSON.toJSONString(garageMap), GarageModel.class);
										mbuser.brandid = g.brandid;
										mbuser.modelid = g.modelid;
										MotoBrandModel res = MotoDataManager.getInstance().getMotoBrand(g.brandid);
										if (res != null) {
											mbuser.brandparentid = MotoDataManager.getInstance().getMotoBrand(g.brandid).bpid;
										}
									}
									if (map.containsKey("addtime")) {
										mbuser.addtime = Long.parseLong(map.get("addtime").toString());
									}

									StringBuffer lastActiveTimeSQL = new StringBuffer();
//									String tableName=D
									// 一年内没有登录过 标记为无效用户
									int year = 12;
									lastActiveTimeSQL.append("select * from  (");

									for (int i = 0; i < year; i++) {
										lastActiveTimeSQL.append("select * from userloginonlog");
										lastActiveTimeSQL.append(LocalDate.now().plusMonths(-i).format(DateTimeFormatter.ofPattern("_yyyy_M")));
										lastActiveTimeSQL.append(" where userid=\"" + userid + "\" and ctype in (1,2) ");
										if (i != year - 1) {
											lastActiveTimeSQL.append(" \r\n UNION ALL  \r\n");
										}
									}
									lastActiveTimeSQL.append(") as  t ORDER BY t.logintime limit 1");

									List<Map<String, Object>> lastActiveTimeMapList = UserDAO.executesql(lastActiveTimeSQL.toString());
									if (!CollectionUtil.isEmpty(lastActiveTimeMapList)) {
										Map<String, Object> lastActiveTime = lastActiveTimeMapList.get(0);
										if (lastActiveTime.containsKey("ctype")) {
											mbuser.ctype = Integer.parseInt(lastActiveTime.get("ctype").toString());
										}
										if (lastActiveTime.containsKey("cversion")) {
											String cversion = lastActiveTime.get("cversion").toString().replaceAll("\\.", "");
											mbuser.cversion = Long.parseLong(cversion);
										}
										if (lastActiveTime.containsKey("logintime")) {
											mbuser.lastactivetime = Long.parseLong(lastActiveTime.get("logintime").toString());
										}
										checkTimStatus(mbuser);
									} else {
										mbuser.state = 1;
									}
									mbuser.updatetime = System.currentTimeMillis();
									isvalidusercount.getAndDecrement();
									pushlist.add(mbuser);
//									LOGGER.info("更新用戶有效性over mbuser="+JSON.toJSONString(mbuser));
									LOGGER.info("userid="+userid+",检测结束,state="+mbuser.state);
								}
								
								UserDAO.inserUserPushBatch(pushlist);
							} catch (Exception e) {
								LOGGER.error(e);
							}finally {
								try {
									cb.await();
								} catch (InterruptedException | BrokenBarrierException e) {
									LOGGER.error(e);
								}
							}
							
						}
					});
				}
				
				cb.await();
				if(result.size()<pagesize) {
					break;
				}
				start+=pagesize;

			}
			
			LOGGER.info("更新用戶有效性over,isvalidusercount="+isvalidusercount);

		} catch (Exception e) {
				if(e instanceof InterruptedException) {
        		return null;
        	}else {
        		LOGGER.error("ERROR="+ExceptionUtils.getStackTrace(e));
                return new Result(Action.EXECUTE_FAILED, ExceptionUtils.getStackTrace(e));
        	}
		}
		return  new Result(Action.EXECUTE_SUCCESS,"更新成功的有效用户数:"+isvalidusercount);
	}

	private void checkTimStatus(MBUserPushModel mbuser) {
		List<Map<String,Object>> list3=Lists.newArrayList();
		Map<String,Object> useridparams=Maps.newHashMap();
		useridparams.put("UserID", mbuser.userid);
		list3.add(useridparams);
		Map<String,Object> params=Maps.newHashMap();
		params.put("CheckItem", list3);
		Map<String,Object> urlMap=new TIMFactory().createTIMUserOpenLoginProduct().checkAccount();
		String url=urlMap.get("url").toString();	
		try {
			Response res=OkHttpClientUtil.okHttpPost(url, JSON.toJSONString(params));
			if(res.isSuccessful()) {
				String str=res.body().string();
				JSONArray jsonArray=JSON.parseObject(str).getJSONArray("ResultItem");
				for (int i=0;i<jsonArray.size();i++) {
					if(jsonArray.getJSONObject(i).getString("AccountStatus").equals("NotImported")) {
						mbuser.state=1;
					}
				}
			}
		} catch (Exception e) {
			LOGGER.error(e);
		}
	}

	@Override
	public void interrupt() {
		LOGGER.info("结束处理...............");
	}
}