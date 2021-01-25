/**
 * 
 */
package com.motobang.task.impl.news;

import java.util.Map;

import com.github.ltsopensource.core.logger.Logger;
import com.github.ltsopensource.core.logger.LoggerFactory;
import com.github.ltsopensource.tasktracker.Result;
import com.github.ltsopensource.tasktracker.runner.JobContext;
import com.github.ltsopensource.tasktracker.runner.JobRunner;
import com.motoband.common.Consts;
import com.motoband.manager.NewsManager;
import com.motoband.manager.RedisManager;
import com.motoband.model.news.NewsModel;
import com.motobang.task.impl.heweather.HEWEATHERNOW;

/**
 * @author 61497
 * 发布动态，同步，数据库，es，车友圈，通知，提到了谁，添加到关注列表，等等
 * @version 创建时间：2021-1-25 18:30:42
 */
public class PUBLISHNEWS implements JobRunner {
	protected static final Logger LOGGER = LoggerFactory.getLogger(HEWEATHERNOW.class);
	@Override
	public Result run(JobContext arg0) throws Throwable {
		LOGGER.info("PUBLISHNEWS is start,job="+com.github.ltsopensource.core.json.JSON.toJSONString(arg0.getJob()));
		String nid=arg0.getJob().getParam("nid");
		String userid=arg0.getJob().getParam("userid");
		Map<String, String> map=null;
		NewsModel model = null;
		map = RedisManager.getInstance().hgetAll(Consts.REDIS_SCHEME_NEWS, nid + NewsManager.NEWSKEY_NEWSINFO);
		if (map != null && !map.isEmpty()) {
			model = NewsModel.convertToModel(map);
			if (model == null || model.nid == null) {
				RedisManager.getInstance().delbykey(Consts.REDIS_SCHEME_NEWS, nid + NewsManager.NEWSKEY_NEWSINFO);
				return null;
			}else {
				NewsManager.getInstance().publishTask(model, userid);
			}
		} else {
			return null;
		}
		return null;
	}

}
