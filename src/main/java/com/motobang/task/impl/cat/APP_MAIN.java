package com.motobang.task.impl.cat;

import java.text.SimpleDateFormat;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.TimeZone;
import java.util.UUID;

import javax.crypto.Mac;
import javax.crypto.spec.SecretKeySpec;

import org.apache.commons.lang3.StringUtils;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.github.ltsopensource.core.logger.Logger;
import com.github.ltsopensource.core.logger.LoggerFactory;
import com.github.ltsopensource.tasktracker.Result;
import com.github.ltsopensource.tasktracker.runner.JobContext;
import com.github.ltsopensource.tasktracker.runner.JobRunner;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.motoband.common.Consts;
import com.motoband.manager.SMSSendManager;
import com.motoband.model.SMSSendLogModel;
import com.motoband.utils.Base64Utils;
import com.motoband.utils.MD5;
import com.motoband.utils.MapUtils;
import com.motoband.utils.OkHttpClientUtil;
import com.motoband.utils.RSAUtils;
import com.motobang.task.impl.newmotomodel.NEWMOTOMODEL_RANK;

import okhttp3.Headers;
import okhttp3.Response;

/**
 * 客户端SHA1WithRSA加签解签demo
 */
public class APP_MAIN implements JobRunner  {
    protected static final Logger LOGGER = LoggerFactory.getLogger(APP_MAIN.class);

	private static final String test_url = "https://api.motuobang.com/release/news/getmainpagelist";
	private static final String secretId = "AKIDKpo6me25b14nzcNefQeoqR95syh2ayx97s0g";
	private static final String secretKey = "5a38htm67thU4xyrLvtektm1OP53FjjfMtw3trf8";
	private static final String CONTENT_CHARSET = "UTF-8";
	private static final String HMAC_ALGORITHM = "HmacSHA1";
	private static final String app_privateKey = "MIIBVQIBADANBgkqhkiG9w0BAQEFAASCAT8wggE7AgEAAkEAo2VMTMp1v/bddhDSRiZsnTnMjN8paDlHKUI6yf+ZZWEg2zzsGQRG+vDSsYC9+FtE06XNEno3SGelhmYhQ6savwIDAQABAkEAhqqwbgHXdnWCJRLMG2ED17mRavFqcSn0Cn85cM6moLRPu/HBKNHrD9Iu+tXgkqXbDu3lrCwCieRUrN1CwRNrQQIhAOJ6CJvTOmBKJ4nEPw1ZGn31fCF6yzVVnb2w9DyUxgsPAiEAuLIhE00Br41lKeM7s7WyhFv5LDj4gS0jL5Moi+HQtVECIQCcjAgpVi/y4S9FXn7LBj12tdqQ9eVDP6QivA+HVLs0ZwIgKRkCASB2ipDE/QAiTcfVlFw+4tc+fMgFd1WghRfXcDECIAtbg9pD4x5JeNvftHntBXlEkyBMGGAl8p0SxCGFW5j6";
	private static final String app_publicKey = "MFwwDQYJKoZIhvcNAQEBBQADSwAwSAJBAKNlTEzKdb/23XYQ0kYmbJ05zIzfKWg5RylCOsn/mWVhINs87BkERvrw0rGAvfhbRNOlzRJ6N0hnpYZmIUOrGr8CAwEAAQ==";
	private static final String server_privateKey = "MIIBVAIBADANBgkqhkiG9w0BAQEFAASCAT4wggE6AgEAAkEAk7aL0Erocm5wA//vWpccwTPYlxON+8L7ap498aEIqIJh0wc55188n9AdIX+IzYbu0NTmEtNXXZ6SujaCNbqjSwIDAQABAkAefQQ4tu1piR/3w2DNCvm1axUegbvBpkoshIL2l61o0kgYlUFQKUqG58pkAFbqPF3HXsAFf981NfWqJLgsCo0RAiEAz98kMcbZrB1dC4AzufeOjIMX4Tj9WvPwG4JBj6nV7qMCIQC16beoSEp3nowZ536V6shVNmTIXI28RifpFsryV8KLOQIgRj1vXIaFzPvLEfTLBb7Z4y705by9F3uwGYuFHcdUq6MCIHOLly8Zc8tU9lJB2wKoVUTivtMRDqnmudTwuEmD+rrpAiEAhcAgEWRgeICKL0WSKcofIqy+5TypCuxkvLvGasX79G4=";
	private static final String server_publicKey = "MFwwDQYJKoZIhvcNAQEBBQADSwAwSAJBAJO2i9BK6HJucAP/71qXHMEz2JcTjfvC+2qePfGhCKiCYdMHOedfPJ/QHSF/iM2G7tDU5hLTV12ekro2gjW6o0sCAwEAAQ==";

	public static void main(String[] args) throws Exception {
	

	}

	public static String sign(String secret, String timeStr) throws Exception {
		// get signStr
		String signStr = "date: " + timeStr + "\n" + "source: " + "source";
		// get sig
		String sig = null;
		Mac mac1 = Mac.getInstance(HMAC_ALGORITHM);
		byte[] hash;
		SecretKeySpec secretKey = new SecretKeySpec(secret.getBytes(CONTENT_CHARSET), mac1.getAlgorithm());
		mac1.init(secretKey);
		hash = mac1.doFinal(signStr.getBytes(CONTENT_CHARSET));
		sig = new String(Base64Utils.encode(hash));
		System.out.println("signValue--->" + sig);
		return sig;
	}

	@Override
	public Result run(JobContext jobContext) throws Throwable {
		String responseStr = null;
		String getmainpagestr="{\"pagetype\":0,\"ctype\":\"2\",\"citycode\":\"010\",\"requestid\":\"101201912101943045619795FECD50175963\",\"sign\":\"IydkTxj0kDgbFy4t08IaKZk4hKwXDETbrMAasoIOu88fSKSD0FGN8ck05U6E9iPzIRwJU4+gcjeSoiMNwj2kyQ==\",\"cversion\":\"3.8.5.19112301\",\"userid\":\"E2CD901A398C4B66A4BB16CE1603B3B1\",\"token\":\"C54AC7EBB155430F976B43A6117D2D79\",\"lonlat\":\"[116.465921,40.01821]\"}";
		Map<String, Object> map = JSON.parseObject(getmainpagestr);
		if(map.get("sign")!=null) {
			map.remove("sign");
		}
		String content = RSAUtils.getSignContent(map);
		// 2.将拼接后的字符串用MD5签名,生成MD5值
		String md5str = MD5.stringToMD5(content);
		// 3.用客户端私钥对MD5值进行RSA签名，生成密文
//		try {
			String sign_ = RSAUtils.sign(md5str.getBytes(), app_privateKey);
			// 4.将密文拼接在请求参数中,key暂定为sign_
			map.put("sign", sign_);
//			System.out.println(sign_);
			String value=JSON.toJSONString(map);
			System.out.println(value);
			Calendar cd = Calendar.getInstance();
			SimpleDateFormat sdf = new SimpleDateFormat("EEE, dd MMM yyyy HH:mm:ss 'GMT'", Locale.US);
			sdf.setTimeZone(TimeZone.getTimeZone("GMT"));
			String timeStr = sdf.format(cd.getTime());
			String sig = sign(secretKey, timeStr);
			String authen = "hmac id=\"" + secretId + "\", algorithm=\"hmac-sha1\", headers=\"date source\", signature=\"" + sig + "\"";
			System.out.println(authen);
			Headers headers = new Headers.Builder().add("Date", timeStr).add("Source", "source").add("Authorization", authen).build();
			Response response = OkHttpClientUtil.okHttpPost(test_url, MapUtils.convertValueString(map), headers);
			if (response.isSuccessful() && response.code() == 200) {
				responseStr = response.body().string();
				System.out.println("responseStr:" + responseStr + ",headers:" + response.headers());

			} else {
				responseStr = response.body().string();
				System.out.println("error:" + responseStr + ",headers:" + response.headers());
				ArrayList<String> params = new ArrayList<String>();
				try {
					if (StringUtils.isNotBlank(Consts.TECHNICIST_ADMINS)) {
						String[] technicist_arr = Consts.TECHNICIST_ADMINS.split(",");
						if (technicist_arr != null && technicist_arr.length > 0) {
							for (int i = 0; i < technicist_arr.length; i++) {
								SMSSendLogModel smsSendLogModel = SMSSendManager.getInstance().send(622572, "86", technicist_arr[i], params);
								smsSendLogModel.sendtime = System.currentTimeMillis();
								smsSendLogModel.sendtype = 30;
								smsSendLogModel.nationcode = Integer.parseInt("86");
								SMSSendManager.getInstance().addSendLog(smsSendLogModel);
							}
						}

					}

				} catch (Throwable e) {
					LOGGER.error("发送首页接口异常手机短信error!");
					LOGGER.error("发送首页接口异常手机短信error!");
					LOGGER.error("发送首页接口异常手机短信error!");
//					ErrorLog(responseWrapper, path, jsonStr, e, content);
				}
			}
//		} catch (Exception e) {
//			// 签名失败
//			e.printStackTrace();
//		}

		/***************************************************************************
		 * 解签部分
		 ************************************************************/
//		if (StringUtils.isNotBlank(responseStr)) {
//			Map<String, Object> responseMap = JSON.parseObject(responseStr);
//			String sign_ = (String) responseMap.get("sign_");
//			if (StringUtils.isNotBlank(sign_)) {
//				// 1 移除sign_
//				responseMap.remove("sign_");
//				// 2.请求参数中所有1级key按照a-z的顺序排序,首字母相同看下一位，以此类推，拼接后的key,value字符串可参考ECOM
//				String waitVerifyStr = RSAUtil.getSignContent(responseMap);
//				// 3.将拼接后的字符串用MD5签名,生成MD5值
//				String md5Str = MD5.stringToMD5(waitVerifyStr);
//				// 4.用服务器公钥对MD5和sign_进行RSA签名校验
//				boolean flag = RSAUtil.verify(md5Str.getBytes("utf-8"), server_publicKey, sign_);
//				if (!flag) {
//					// 客户端不予渲染
//					System.out.println("未信任的服务器");
//				} else {
//					System.out.println(responseStr);
//				}
//			} else {
//				// 客户端不予渲染
//				System.out.println("未信任的服务器");
//			}
//		}
		return null;
	}
}
