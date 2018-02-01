package com.jason.transformer.mr.am;

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

/**
 * mapreduce程序中计算active member的mapper类<br/>
 * 其实就是按照维度信息进行分组输出
 * 
 * @author gerry
 *
 */
public class ActiveMemberMapper extends TableMapper<StatsUserDimension, TimeOutputValue> {
    private static final Logger logger = Logger.getLogger(ActiveMemberMapper.class);
    public static final byte[] family = Bytes.toBytes(EventLogConstants.EVENT_LOGS_FAMILY_NAME);
    private StatsUserDimension outputKey = new StatsUserDimension();
    private TimeOutputValue outputValue = new TimeOutputValue();
    private BrowserDimension defaultBrowser = new BrowserDimension("", ""); // 默认的browser对象
    private KpiDimension activeMemberKpi = new KpiDimension(KpiType.ACTIVE_MEMBER.name);
    private KpiDimension activeMemberOfBrowserKpi = new KpiDimension(KpiType.BROWSER_ACTIVE_MEMBER.name);

    @Override
    protected void map(ImmutableBytesWritable key, Result value, Context context) throws IOException, InterruptedException {
        // 获取u_mid&platform&serverTime，从hbase返回的结果集Result中
        String memberId = Bytes.toString(value.getValue(family, Bytes.toBytes(EventLogConstants.LOG_COLUMN_NAME_MEMBER_ID)));
        String platform = Bytes.toString(value.getValue(family, Bytes.toBytes(EventLogConstants.LOG_COLUMN_NAME_PLATFORM)));
        String serverTime = Bytes.toString(value.getValue(family, Bytes.toBytes(EventLogConstants.LOG_COLUMN_NAME_SERVER_TIME)));

        // 过滤无效数据
        if (StringUtils.isBlank(memberId) || StringUtils.isBlank(platform) || StringUtils.isBlank(serverTime) || !StringUtils.isNumeric(serverTime.trim())) {
            System.out.println(Bytes.toString(value.getRow()));
            logger.warn("memberId&platform&serverTime不能为空，而且serverTime必须为时间戳");
            return;
        }

        long longOfServerTime = Long.valueOf(serverTime.trim());
        DateDimension dateDimension = DateDimension.buildDate(longOfServerTime, DateEnum.DAY);
        this.outputValue.setId(memberId);

        // 进行platform的构建
        List<PlatformDimension> platforms = PlatformDimension.buildList(platform); // 进行platform创建
        // 获取browser name和browser version
        String browser = Bytes.toString(value.getValue(family, Bytes.toBytes(EventLogConstants.LOG_COLUMN_NAME_BROWSER_NAME)));
        String browserVersion = Bytes.toString(value.getValue(family, Bytes.toBytes(EventLogConstants.LOG_COLUMN_NAME_BROWSER_VERSION)));
        // 进行browser的维度信息构建
        List<BrowserDimension> browsers = BrowserDimension.buildList(browser, browserVersion);

        // 开始进行输出
        StatsCommonDimension statsCommonDimension = this.outputKey.getStatsCommon();
        // 设置date dimension
        statsCommonDimension.setDate(dateDimension);
        for (PlatformDimension pf : platforms) {
            this.outputKey.setBrowser(defaultBrowser); // 进行覆盖操作
            // 设置platform dimension
            statsCommonDimension.setPlatform(pf);
            // 设置kpi dimension
            statsCommonDimension.setKpi(activeMemberKpi);
            context.write(this.outputKey, this.outputValue);

            // 输出browser维度统计
            statsCommonDimension.setKpi(activeMemberOfBrowserKpi);
            for (BrowserDimension bw : browsers) {
                this.outputKey.setBrowser(bw); // 设置对应的browsers
                context.write(this.outputKey, this.outputValue);
            }
        }
    }
}
