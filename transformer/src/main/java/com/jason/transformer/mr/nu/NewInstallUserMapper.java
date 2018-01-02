package com.jason.transformer.mr.nu;

import java.io.IOException;
import java.util.List;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.log4j.Logger;

import com.jason.common.DateEnum;
import com.jason.common.EventLogConstants;
import com.jason.common.KpiType;
import com.jason.transformer.model.dim.StatsCommonDimension;
import com.jason.transformer.model.dim.StatsUserDimension;
import com.jason.transformer.model.dim.base.BrowserDimension;
import com.jason.transformer.model.dim.base.DateDimension;
import com.jason.transformer.model.dim.base.KpiDimension;
import com.jason.transformer.model.dim.base.PlatformDimension;
import com.jason.transformer.model.value.map.TimeOutputValue;
import com.jason.transformer.mr.TransformerBaseMapper;

/**
 * 自定义的计算新用户的mapper类
 * 
 * @author jason
 *
 */
public class NewInstallUserMapper extends TransformerBaseMapper<StatsUserDimension, TimeOutputValue> {
    private static final Logger logger = Logger.getLogger(NewInstallUserMapper.class);
    private StatsUserDimension statsUserDimension = new StatsUserDimension(); //(用户基本分析和浏览器分析)定义的组合维度
    private TimeOutputValue timeOutputValue = new TimeOutputValue();  //设置id和时间的工具类
    
    private KpiDimension newInstallUserKpi = new KpiDimension(KpiType.NEW_INSTALL_USER.name); //统计kpi的名称
    private KpiDimension newInstallUserOfBrowserKpi = new KpiDimension(KpiType.BROWSER_NEW_INSTALL_USER.name);

    @Override
    protected void map(ImmutableBytesWritable key, Result value, Context context) throws IOException, InterruptedException {
    	this.inputRecords++;
        String uuid = super.getUuid(value);
        String serverTime = super.getServerTime(value);
        String platform = super.getPlatform(value);
        if (StringUtils.isBlank(uuid) || StringUtils.isBlank(serverTime) || StringUtils.isBlank(platform)) {
            logger.warn("uuid&servertime&platform不能为空");
            this.filterRecords++;
            return;
        }
        long longOfTime = Long.valueOf(serverTime.trim());
        timeOutputValue.setId(uuid); // 设置id为uuid
        timeOutputValue.setTime(longOfTime); // 设置时间为服务器时间
        DateDimension dateDimension = DateDimension.buildDate(longOfTime, DateEnum.DAY);
        List<PlatformDimension> platformDimensions = PlatformDimension.buildList(platform);

        // 设置date维度
        StatsCommonDimension statsCommonDimension = this.statsUserDimension.getStatsCommon(); //date,platform,kpi
        statsCommonDimension.setDate(dateDimension);
        // 写browser相关的数据
        String browserName = super.getBrowserName(value);
        String browserVersion = super.getBrowserVersion(value);
        //根据browser的不同设置不同的list
        List<BrowserDimension> browserDimensions = BrowserDimension.buildList(browserName, browserVersion);
        BrowserDimension defaultBrowser = new BrowserDimension("", "");
        for (PlatformDimension pf : platformDimensions) {
            // 1. 设置为一个默认值
            statsUserDimension.setBrowser(defaultBrowser);
            // 2. 解决有空的browser输出的bug
            // statsUserDimension.getBrowser().clean();
            statsCommonDimension.setKpi(newInstallUserKpi);
            statsCommonDimension.setPlatform(pf);
            context.write(statsUserDimension, timeOutputValue);  //用户基本信息维度，uuid+servertime
            this.outputRecords++;
            for (BrowserDimension br : browserDimensions) {
                statsCommonDimension.setKpi(newInstallUserOfBrowserKpi);
                // 1. 
                statsUserDimension.setBrowser(br);
                // 2. 由于上面需要进行clean操作，故将该值进行clone后填充
                // statsUserDimension.setBrowser(WritableUtils.clone(br, context.getConfiguration()));
                context.write(statsUserDimension, timeOutputValue);
                this.outputRecords++;
            }
        }
    }
}
