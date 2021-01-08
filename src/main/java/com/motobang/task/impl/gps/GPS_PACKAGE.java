package com.motobang.task.impl.gps;


import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Calendar;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import com.alibaba.fastjson.JSON;
import com.github.ltsopensource.core.logger.Logger;
import com.github.ltsopensource.core.logger.LoggerFactory;
import com.github.ltsopensource.tasktracker.Result;
import com.github.ltsopensource.tasktracker.runner.InterruptibleJobRunner;
import com.github.ltsopensource.tasktracker.runner.JobContext;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.motoband.common.Consts;
import com.motoband.dao.gps.HardwareGPSDao;
import com.motoband.manager.RedisManager;
import com.motoband.manager.hardware.gps.parse.EFullUploadReport;
import com.motoband.model.hardware.gps.GPSBaseReportInfoModel;
import com.motoband.utils.OkHttpClientUtil;
import com.motoband.utils.collection.CollectionUtil;
import com.qcloud.cos.COSClient;
import com.qcloud.cos.ClientConfig;
import com.qcloud.cos.auth.BasicCOSCredentials;
import com.qcloud.cos.auth.COSCredentials;
import com.qcloud.cos.model.ObjectMetadata;
import com.qcloud.cos.model.PutObjectRequest;
import com.qcloud.cos.model.UploadResult;
import com.qcloud.cos.region.Region;
import com.qcloud.cos.transfer.TransferManager;
import com.qcloud.cos.transfer.TransferManagerConfiguration;
import com.qcloud.cos.transfer.Upload;
import com.tencentcloudapi.cdn.v20180606.CdnClient;
import com.tencentcloudapi.cdn.v20180606.models.PurgePathCacheRequest;
import com.tencentcloudapi.cdn.v20180606.models.PurgePathCacheResponse;
import com.tencentcloudapi.common.Credential;
import com.tencentcloudapi.common.profile.ClientProfile;
import com.tencentcloudapi.common.profile.HttpProfile;

public class GPS_PACKAGE  implements InterruptibleJobRunner {
    protected static final Logger LOGGER = LoggerFactory.getLogger(GPS_PACKAGE.class);

	@Override
	public Result run(JobContext jobContext) throws Throwable {
		LOGGER.info("GPS_PACKAGE is start");
		long max=System.currentTimeMillis();
		Set<String> rdSet=RedisManager.getInstance().zrangbyscore(Consts.REDIS_SCHEME_RUN, EFullUploadReport.GPS_PACKAGE_SET, 0, max);
		int fresh=0;
//		rdSet=Sets.newHashSet("0D34DF5421836379C33D145317AEBF0B");
		if(CollectionUtil.isNotEmpty(rdSet)) {
			COSCredentials cred = new BasicCOSCredentials(Consts.SECRETID, Consts.SECRETKEY);
			// 2 设置 bucket 的区域, COS 地域的简称请参照 https://cloud.tencent.com/document/product/436/6224
			// clientConfig 中包含了设置 region, https(默认 http), 超时, 代理等 set 方法, 使用可参见源码或者常见问题 Java SDK 部分。
			Region region = new Region("ap-beijing");
			ClientConfig clientConfig = new ClientConfig(region);
			// 3 生成 cos 客户端。
			COSClient cosclient= new COSClient(cred, clientConfig);		
			String bucketName = "gpsridelinedata-1251739791";
			ExecutorService threadPool = Executors.newFixedThreadPool(16);
			// 传入一个 threadpool, 若不传入线程池，默认 TransferManager 中会生成一个单线程的线程池。
			TransferManager transferManager = new TransferManager(cosclient, threadPool);
			// 设置高级接口的分块上传阈值和分块大小为10MB
			TransferManagerConfiguration transferManagerConfiguration = new TransferManagerConfiguration();
			transferManagerConfiguration.setMultipartUploadThreshold(10 * 1024 * 1024);
			transferManagerConfiguration.setMinimumUploadPartSize(10 * 1024 * 1024);
			transferManager.setConfiguration(transferManagerConfiguration);
			for (String rd : rdSet) {
				try {
					fresh=1;
					Map<String,Object> map=Maps.newHashMap();
					map.put("rd", rd);
//					map.put("orderby", "ASC");
//					map.put("valid", 1);
					String key="gpsridelinedata/"+rd+".mbdata";
//					map.put("head", 8);
					List<GPSBaseReportInfoModel> list=HardwareGPSDao.getGPSPackageReportInfoList(map);
					if(CollectionUtil.isNotEmpty(list)) {
						   ObjectMetadata objectMetadata = new ObjectMetadata();
					        // 从输入流上传必须制定content length, 否则http客户端可能会缓存所有数据，存在内存OOM的情况
					        // 默认下载时根据cos路径key的后缀返回响应的contenttype, 上传时设置contenttype会覆盖默认值
					        objectMetadata.setContentType("application/json");
//					        Iterator<GPSBaseReportInfoModel> it=list.iterator();
//					        while (it.hasNext()) {
//					        	GPSBaseReportInfoModel result=it.next();
//					        	if(result.gps.tm<0){
//					        		it.remove();
//					        	}
//							}
					        String json=JSON.toJSONString(list);
					        objectMetadata.setContentLength(json.getBytes().length);
					        InputStream input = new ByteArrayInputStream(json.getBytes());
							PutObjectRequest putObjectRequest = new PutObjectRequest(bucketName, key, input, objectMetadata);
							Upload putObjectResult=transferManager.upload(putObjectRequest);
							UploadResult res=putObjectResult.waitForUploadResult();
//							PutObjectResult putObjectResult = cosclient.putObject(putObjectRequest);	
							String dataurl="https://gpsridelinedata-1251739791.file.myqcloud.com/"+key;
							
							RedisManager.getInstance().zrem(Consts.REDIS_SCHEME_RUN, EFullUploadReport.GPS_PACKAGE_SET, rd);
							HardwareGPSDao.updateGPSRidelineDateurl(rd,dataurl);
							
//						String reportjsonstr=RedisManager.getInstance().string_get(Consts.REDIS_SCHEME_RUN, rd+EFullUploadReport.GPS_REPORT_INFO);
//						GPSBaseReportInfoModel report = JSON.parseObject(reportjsonstr, GPSBaseReportInfoModel.class);
//						GarageModel garagemodel = UserGarageDAO.getUserGaragesBygpssn(report.sn);
//						if (garagemodel == null || StringUtils.isBlank(garagemodel.userid)) {
//							return null;
//						}
//						report.head="8";
//						report.wm=2;
//						report.eng=2;
//						HardwareGPSDao.insertBaseUploadReportInfo(report);
//						new EFullUploadReport().countGPS(report);

					}
				} catch (Exception e) {
					LOGGER.error(e);
					continue;
				}

			}
			//降低cdn目录刷新预热频率，因为有数量限制  8*8=64次/天    腾讯云限制 100次
			if(fresh==1) {
				Calendar calendar = Calendar.getInstance();
				int curHour24 = calendar.get(calendar.HOUR_OF_DAY);
				if(curHour24==7||curHour24==8||curHour24==9||curHour24==10||curHour24==11||curHour24==18||curHour24==19||curHour24==20) {
					int minute=calendar.get(calendar.MINUTE);
					if(minute==7||minute==8||minute==9||minute==10||minute==11||minute==18||minute==19||minute==20) {
						refreshCDN();
					}
				}
				
			}
			
//			RedisManager.getInstance().zremrangeByScore(Consts.REDIS_SCHEME_RUN, EFullUploadReport.GPS_REPORT_INFO_SET, 0, max);
			transferManager.shutdownNow();
			cosclient.shutdown();
			
		}
			

		LOGGER.info("GPS_PACKAGE is end");
		return null;
	}
	//刷新目录
	public void refreshCDN() {
        try{

            Credential cred = new Credential(Consts.SECRETID, Consts.SECRETKEY);

            HttpProfile httpProfile = new HttpProfile();
            httpProfile.setEndpoint("cdn.tencentcloudapi.com");

            ClientProfile clientProfile = new ClientProfile();
            clientProfile.setHttpProfile(httpProfile);

            CdnClient client = new CdnClient(cred, "", clientProfile);

            PurgePathCacheRequest req = new PurgePathCacheRequest();
            String[] paths1 = {"https://gpsridelinedata-1251739791.file.myqcloud.com/gpsridelinedata/"};
            req.setPaths(paths1);

            req.setFlushType("flush");

            PurgePathCacheResponse resp = client.PurgePathCache(req);

            System.out.println(PurgePathCacheResponse.toJsonString(resp));
        } catch (Exception e) {
        	LOGGER.error(e);
        }

    }

	@Override
	public void interrupt() {
		LOGGER.error("中断");
	}

}
