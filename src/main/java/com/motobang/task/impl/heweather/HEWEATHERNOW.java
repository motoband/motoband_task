/**
 * 
 */
package com.motobang.task.impl.heweather;

import com.github.ltsopensource.core.logger.Logger;
import com.github.ltsopensource.core.logger.LoggerFactory;
import com.github.ltsopensource.tasktracker.Result;
import com.github.ltsopensource.tasktracker.runner.JobContext;
import com.github.ltsopensource.tasktracker.runner.JobRunner;
import com.motoband.common.Consts;
import com.motoband.manager.heweather.HeWeatherManager;

/**
 * @author zhanyi
 *2020-7-17
 *和风天气实况同步，一个小时一次，和风数据同步频率 10-20分钟
 */
public class HEWEATHERNOW implements JobRunner{
	protected static final Logger LOGGER = LoggerFactory.getLogger(HEWEATHERNOW.class);

	@Override
	public Result run(JobContext arg0) throws Throwable {
		// TODO Auto-generated method stub
		LOGGER.info("HEWEATHERNOW is started");
//		Consts.MOTOBAND_SEARCHSERVICE="http://10.0.0.11:8091/motoband-search/";
		HeWeatherManager.getInstance().updateCityWeather();
		HeWeatherManager.getInstance().updateWeatherForecast();
		LOGGER.info("HEWEATHERNOW is finished");
		return null;
	}

}