package com.motobang.task.impl.search;

import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang3.StringUtils;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.serializer.SerializerFeature;
import com.github.ltsopensource.core.logger.Logger;
import com.github.ltsopensource.core.logger.LoggerFactory;
import com.github.ltsopensource.tasktracker.Result;
import com.github.ltsopensource.tasktracker.runner.JobContext;
import com.github.ltsopensource.tasktracker.runner.JobRunner;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.google.common.reflect.TypeToken;
import com.motoband.common.Consts;
import com.motoband.manager.MotoDataManager;
import com.motoband.manager.YZManager;
import com.motoband.manager.search.ElasticSearchManager;
import com.motoband.model.MallProductModel;
import com.motoband.model.NewMotoModel;
import com.motoband.model.NewMotoModelV2;
import com.motoband.model.SuggestModel;
import com.motoband.utils.MD5;
import com.motoband.utils.OkHttpClientUtil;
import com.motoband.utils.collection.CollectionUtil;

public class SEARCH_SUGGEST implements JobRunner  {
    protected static final Logger LOGGER = LoggerFactory.getLogger(SEARCH_SUGGEST.class);

	@Override
	public Result run(JobContext jobContext) throws Throwable {
		LOGGER.error("SEARCH_SUGGEST is start");
		Consts.MOTOBAND_SEARCHSERVICE="http://10.0.0.11:8091/motoband-search/";
		String urlString =  "http://10.0.0.11:8091/motoband-search/search/suggest/removeall";
//		String urlString =  "http://127.0.0.1:8091/motoband-search/search/suggest/removeall";
		Map<String,Object> map=Maps.newHashMap();
//		Map<String, String> requestData = new HashMap<String, String>();
//		requestData.put("searchcontent", searchcontent);
		List<String> list = Lists.newArrayList();
		List<Map<String, Object>> resultList = Lists.newArrayList();
		list = OkHttpClientUtil.okHttpPost(urlString, JSON.toJSONString(map), new TypeToken<List<String>>() {
		}.getType());		
		handleLabels();
		LOGGER.info("handleLabels is  sync over");
		handleMoto();
		LOGGER.info("handleMoto is  sync over");
		handleMall();
		LOGGER.info("handleMall is  sync over");
		return null;
	}
	
	private void handleMall() {
		List<MallProductModel> r=Lists.newArrayList();
		List<String> list=Lists.newArrayList();
		int pageno=1;
		do {
			r=YZManager.getInstance().searchYzProduct(pageno, 300, null);
			for (MallProductModel model : r) {
				list.add(model.title);
			}
			pageno++;
			LOGGER.info("handleMall pageno="+pageno);
			if(r.size()==0){
				break;
			}
		} while (r.size()<=300);
		List<List<String>> averageList=CollectionUtil.averageAssign(list, list.size()/500+1);
		for (List<String> list2 : averageList) {
			Map<String,Object> params=Maps.newHashMap();
			List<String> sids=Lists.newArrayList();
			List<String> searchcontent=Lists.newArrayList();

			for (String key : list2) {
				if(StringUtils.isNotBlank(key)){
					SuggestModel sm=new SuggestModel();
					sm.malldesc=key;
					sm.modeldesc=null;
					sm.topicdesc=null;
					sm.userdesc=null;
					String sid=MD5.stringToMD5(key);
					sm.sid=sid;
					sm.type=3;
					sids.add(sid);
					searchcontent.add(JSON.toJSONString(sm));
				}
			}
			params.put("searchcontent",searchcontent);
			params.put("sids",sids);
			ElasticSearchManager.getInstance().syncAddEsList(SuggestModel.class, JSON.toJSONString(params));
			LOGGER.info("handleMall handle.....");

		}
		
	}

	private void handleMoto() {
		LinkedHashSet<String> list=Sets.newLinkedHashSet();
		List<NewMotoModelV2> newmotomodel=MotoDataManager.getInstance().getNewMotoModelsV2();
		for (NewMotoModelV2 newMotoModel2 : newmotomodel) {
			list.add(newMotoModel2.name);
			list.add(newMotoModel2.brandname);
//			list.add(newMotoModel2.brandparentname);
		}
//		List<List<String>> averageList=CollectionUtil.averageAssign(list, list.size()/500+1);
//		for (String list2 : list) {
			Map<String,Object> params=Maps.newHashMap();
			List<String> sids=Lists.newArrayList();
			List<String> searchcontent=Lists.newArrayList();

			for (String key : list) {
				 searchcontent=Lists.newArrayList();
				 sids=Lists.newArrayList();
				SuggestModel sm=new SuggestModel();
				sm.modeldesc=key;
				sm.topicdesc=null;
				sm.malldesc=null;
				sm.userdesc=null;
				String sid=MD5.stringToMD5(key);
				sm.sid=sid;
				sm.type=2;
				sids.add(sid);
				searchcontent.add(JSON.toJSONString(sm,SerializerFeature.WriteNullStringAsEmpty));
				params.put("searchcontent",searchcontent);
				params.put("sids",sids);
				ElasticSearchManager.getInstance().syncAddEsList(SuggestModel.class, JSON.toJSONString(params));
				LOGGER.info("handleMoto handle.....  key="+key);
			}


//		}
	}

	private void handleLabels() throws Exception {
		String urlString = Consts.MOTOBAND_SEARCHSERVICE + "search/searchlabel";
		Map<String,Object> map=Maps.newHashMap();
		map.put("pagesize", 100000);
//		Map<String, String> requestData = new HashMap<String, String>();
//		requestData.put("searchcontent", searchcontent);
		List<String> list = Lists.newArrayList();
		List<Map<String, Object>> resultList = Lists.newArrayList();
		list = OkHttpClientUtil.okHttpPost(urlString, JSON.toJSONString(map), new TypeToken<List<String>>() {
		}.getType());		
		if (!CollectionUtil.isEmpty(list)) {
			List<List<String>> averageList=CollectionUtil.averageAssign(list, list.size()/500+1);
			for (List<String> list2 : averageList) {
				Map<String,Object> params=Maps.newHashMap();
				List<String> sids=Lists.newArrayList();
				List<String> searchcontent=Lists.newArrayList();

				for (String key : list2) {
					SuggestModel sm=new SuggestModel();
					sm.topicdesc=key;
					sm.modeldesc=null;
					sm.malldesc=null;
					sm.userdesc=null;
					String sid=MD5.stringToMD5(key);
					sm.sid=sid;
					sm.type=1;
					sids.add(sid);
					searchcontent.add(JSON.toJSONString(sm,SerializerFeature.WriteNullStringAsEmpty));
				}
				params.put("searchcontent",searchcontent);
				params.put("sids",sids);
				ElasticSearchManager.getInstance().syncAddEsList(SuggestModel.class, JSON.toJSONString(params));
				LOGGER.info("handleLabels handle.....");

			}
//			for (int i = 0; i < 10; i++) {
//				LOGGER.info(JSON.toJSONString(list.get(i)));
//			}
		}
	}


}
