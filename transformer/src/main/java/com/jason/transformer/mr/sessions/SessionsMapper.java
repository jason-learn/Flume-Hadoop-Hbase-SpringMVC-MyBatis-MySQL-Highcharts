package com.jason.transformer.mr.sessions;

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

public class SessionsMapper extends TableMapper<StatsUserDimension, TimeOutputValue> {
    private static final Logger logger = Logger.getLogger(SessionsMapper.class);
    public static final byte[] family = Bytes.toBytes(EventLogConstants.EVENT_LOGS_FAMILY_NAME);
    private StatsUserDimension outputKey = new StatsUserDimension();
    private TimeOutputValue outputValue = new TimeOutputValue();
    private BrowserDimension defaultBrowserDimension = new BrowserDimension("", "");
    private KpiDimension sessionsKpi = new KpiDimension(KpiType.SESSIONS.name);
    private KpiDimension sessionsOfBrowserKpi = new KpiDimension(KpiType.BROWSER_SESSIONS.name);

    @Override
    protected void map(ImmutableBytesWritable key, Result value, Context context) throws IOException, InterruptedException {
        // 获取会话id，serverTime， 平台
        String sessionId = Bytes.toString(value.getValue(family, Bytes.toBytes(EventLogConstants.LOG_COLUMN_NAME_SESSION_ID)));
        String platform = Bytes.toString(value.getValue(family, Bytes.toBytes(EventLogConstants.LOG_COLUMN_NAME_PLATFORM)));
        String serverTime = Bytes.toString(value.getValue(family, Bytes.toBytes(EventLogConstants.LOG_COLUMN_NAME_SERVER_TIME)));

        // 过滤无效数据
        if (StringUtils.isBlank(sessionId) || StringUtils.isBlank(platform) || StringUtils.isBlank(serverTime) || !StringUtils.isNumeric(serverTime.trim())) {
            logger.warn("会话id&platform&服务器时间不能为空，而且服务器时间必须为时间戳形式.");
            return;
        }

        // 创建date 维度
        long longOfTime = Long.valueOf(serverTime.trim());
        DateDimension dayOfDimension = DateDimension.buildDate(longOfTime, DateEnum.DAY);
        // 创建 platform维度
        List<PlatformDimension> platforms = PlatformDimension.buildList(platform);
        // 创建browser维度
        String browserName = Bytes.toString(value.getValue(family, Bytes.toBytes(EventLogConstants.LOG_COLUMN_NAME_BROWSER_NAME)));
        String browserVersion = Bytes.toString(value.getValue(family, Bytes.toBytes(EventLogConstants.LOG_COLUMN_NAME_BROWSER_VERSION)));
        List<BrowserDimension> browsers = BrowserDimension.buildList(browserName, browserVersion);

        // 进行输出设置
        this.outputValue.setId(sessionId.trim()); // 会话id
        this.outputValue.setTime(longOfTime); // 服务器时间
        StatsCommonDimension statsCommon = this.outputKey.getStatsCommon();
        statsCommon.setDate(dayOfDimension); // 设置时间维度
        for (PlatformDimension pf : platforms) {
            this.outputKey.setBrowser(this.defaultBrowserDimension);
            statsCommon.setPlatform(pf);
            statsCommon.setKpi(this.sessionsKpi);
            context.write(this.outputKey, this.outputValue); // 输出设置

            // browser输出
            statsCommon.setKpi(this.sessionsOfBrowserKpi); // 将kpi更改为输出browser session
            for (BrowserDimension br : browsers) {
                this.outputKey.setBrowser(br);
                context.write(this.outputKey, this.outputValue);
            }
        }
    }
}
