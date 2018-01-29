package com.jason.transformer.mr.nm;

import java.sql.Connection;
import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.MultipleColumnPrefixFilter;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;

import com.jason.common.DateEnum;
import com.jason.common.EventLogConstants;
import com.jason.common.GlobalConstants;
import com.jason.transformer.model.dim.StatsUserDimension;
import com.jason.transformer.model.dim.base.DateDimension;
import com.jason.transformer.model.value.map.TimeOutputValue;
import com.jason.transformer.model.value.reduce.MapWritableValue;
import com.jason.transformer.mr.TransformerOutputFormat;
import com.jason.util.JdbcManager;
import com.jason.util.TimeUtil;
import com.google.common.collect.Lists;

/**
 * 计算新增会员的入口类
 * 
 * @author jason
 *
 */
public class NewMemberRunner implements Tool {
    private static final Logger logger = Logger.getLogger(NewMemberRunner.class);
    private Configuration conf = null;

    public static void main(String[] args) {
        try {
            ToolRunner.run(new Configuration(), new NewMemberRunner(), args);
        } catch (Exception e) {
            logger.error("统计新增会员&总会员失败，出现异常信息.", e);
            throw new RuntimeException("job执行异常", e);
        }
    }

    @Override
    public void setConf(Configuration conf) {
        // 设置自定义配置信息
        conf.addResource("output-collector.xml");
        conf.addResource("query-mapping.xml");
        conf.addResource("transformer-env.xml");
        // 使用hbase的config帮助类来加载hbase相关配置文件
        this.conf = HBaseConfiguration.create(conf);
    }

    @Override
    public Configuration getConf() {
        return this.conf;
    }

    @Override
    public int run(String[] args) throws Exception {
        Configuration conf = this.getConf();
        // 处理参数
        this.processArgs(conf, args);

        // 创建job
        Job job = Job.getInstance(conf, "new_member");

        // 设置job相关配置
        job.setJarByClass(NewMemberRunner.class);
        // 本地运行，参数为false。如果需要线上运行，设置为true。(最后一个参数)
        TableMapReduceUtil.initTableMapperJob(this.initScans(job), NewMemberMapper.class, StatsUserDimension.class, TimeOutputValue.class, job, false);
        job.setReducerClass(NewMemberReducer.class);
        job.setOutputKeyClass(StatsUserDimension.class);
        job.setOutputValueClass(MapWritableValue.class);

        // 设置自定义outputformat
        job.setOutputFormatClass(TransformerOutputFormat.class);
        if (job.waitForCompletion(true)) {
            // job运行成功
            this.calculateTotalMembers(job.getConfiguration());
            return 0;
        } else {
            // job运行失败
            return -1;
        }
    }

    /**
     * 计算总会员
     * 
     * @param conf
     */
    private void calculateTotalMembers(Configuration conf) {
        Connection conn = null;
        PreparedStatement pstmt = null;
        ResultSet rs = null;

        try {
            long date = TimeUtil.parseString2Long(conf.get(GlobalConstants.RUNNING_DATE_PARAMES));
            // 获取今天的date dimension
            DateDimension todayDimension = DateDimension.buildDate(date, DateEnum.DAY);
            // 获取昨天的date dimension
            DateDimension yesterdayDimension = DateDimension.buildDate(date - GlobalConstants.DAY_OF_MILLISECONDS, DateEnum.DAY);
            int yesterdayDimensionId = -1;
            int todayDimensionId = -1;

            // 1. 获取时间id
            conn = JdbcManager.getConnection(conf, GlobalConstants.WAREHOUSE_OF_REPORT);
            // 获取执行时间的昨天的
            pstmt = conn.prepareStatement("SELECT `id` FROM `dimension_date` WHERE `year` = ? AND `season` = ? AND `month` = ? AND `week` = ? AND `day` = ? AND `type` = ? AND `calendar` = ?");
            int i = 0;
            pstmt.setInt(++i, yesterdayDimension.getYear());
            pstmt.setInt(++i, yesterdayDimension.getSeason());
            pstmt.setInt(++i, yesterdayDimension.getMonth());
            pstmt.setInt(++i, yesterdayDimension.getWeek());
            pstmt.setInt(++i, yesterdayDimension.getDay());
            pstmt.setString(++i, yesterdayDimension.getType());
            pstmt.setDate(++i, new Date(yesterdayDimension.getCalendar().getTime()));
            rs = pstmt.executeQuery();
            if (rs.next()) {
                yesterdayDimensionId = rs.getInt(1);
            }

            // 获取执行时间当天的id
            pstmt = conn.prepareStatement("SELECT `id` FROM `dimension_date` WHERE `year` = ? AND `season` = ? AND `month` = ? AND `week` = ? AND `day` = ? AND `type` = ? AND `calendar` = ?");
            i = 0;
            pstmt.setInt(++i, todayDimension.getYear());
            pstmt.setInt(++i, todayDimension.getSeason());
            pstmt.setInt(++i, todayDimension.getMonth());
            pstmt.setInt(++i, todayDimension.getWeek());
            pstmt.setInt(++i, todayDimension.getDay());
            pstmt.setString(++i, todayDimension.getType());
            pstmt.setDate(++i, new Date(todayDimension.getCalendar().getTime()));
            rs = pstmt.executeQuery();
            if (rs.next()) {
                todayDimensionId = rs.getInt(1);
            }

            // 2.获取昨天的原始数据,存储格式为:platformid = totalusers
            Map<String, Integer> oldValueMap = new HashMap<String, Integer>();

            // 开始更新stats_user
            if (yesterdayDimensionId > -1) {
                pstmt = conn.prepareStatement("select `platform_dimension_id`,`total_members` from `stats_user` where `date_dimension_id`=?");
                pstmt.setInt(1, yesterdayDimensionId);
                rs = pstmt.executeQuery();
                while (rs.next()) {
                    int platformId = rs.getInt("platform_dimension_id");
                    int totalMembers = rs.getInt("total_members");
                    oldValueMap.put("" + platformId, totalMembers);
                }
            }

            // 添加今天的总用户
            pstmt = conn.prepareStatement("select `platform_dimension_id`,`new_members` from `stats_user` where `date_dimension_id`=?");
            pstmt.setInt(1, todayDimensionId);
            rs = pstmt.executeQuery();
            while (rs.next()) {
                int platformId = rs.getInt("platform_dimension_id");
                int newMembers = rs.getInt("new_members");
                if (oldValueMap.containsKey("" + platformId)) {
                    newMembers += oldValueMap.get("" + platformId);
                }
                oldValueMap.put("" + platformId, newMembers);
            }

            // 更新操作
            pstmt = conn.prepareStatement("INSERT INTO `stats_user`(`platform_dimension_id`,`date_dimension_id`,`total_members`) VALUES(?, ?, ?) ON DUPLICATE KEY UPDATE `total_members` = ?");
            for (Map.Entry<String, Integer> entry : oldValueMap.entrySet()) {
                pstmt.setInt(1, Integer.valueOf(entry.getKey()));
                pstmt.setInt(2, todayDimensionId);
                pstmt.setInt(3, entry.getValue());
                pstmt.setInt(4, entry.getValue());
                pstmt.execute();
            }

         // 开始更新stats_device_browser
            oldValueMap.clear();
            if (yesterdayDimensionId > -1) {
                pstmt = conn.prepareStatement("select `platform_dimension_id`,`browser_dimension_id`,`total_members` from `stats_device_browser` where `date_dimension_id`=?");
                pstmt.setInt(1, yesterdayDimensionId);
                rs = pstmt.executeQuery();
                while (rs.next()) {
                    int platformId = rs.getInt("platform_dimension_id");
                    int browserId = rs.getInt("browser_dimension_id");
                    int totalMembers = rs.getInt("total_members");
                    oldValueMap.put(platformId + "_" + browserId, totalMembers);
                }
            }

            // 添加今天的总用户
            pstmt = conn.prepareStatement("select `platform_dimension_id`,`browser_dimension_id`,`new_members` from `stats_device_browser` where `date_dimension_id`=?");
            pstmt.setInt(1, todayDimensionId);
            rs = pstmt.executeQuery();
            while (rs.next()) {
                int platformId = rs.getInt("platform_dimension_id");
                int browserId = rs.getInt("browser_dimension_id");
                int newMembers = rs.getInt("new_members");
                String key = platformId + "_" + browserId;
                if (oldValueMap.containsKey(key)) {
                    newMembers += oldValueMap.get(key);
                }
                oldValueMap.put(key, newMembers);
            }

            // 更新操作
            pstmt = conn.prepareStatement("INSERT INTO `stats_device_browser`(`platform_dimension_id`,`browser_dimension_id`,`date_dimension_id`,`total_members`) VALUES(?, ?, ?, ?) ON DUPLICATE KEY UPDATE `total_members` = ?");
            for (Map.Entry<String, Integer> entry : oldValueMap.entrySet()) {
                String[] key = entry.getKey().split("_");
                pstmt.setInt(1, Integer.valueOf(key[0]));
                pstmt.setInt(2, Integer.valueOf(key[1]));
                pstmt.setInt(3, todayDimensionId);
                pstmt.setInt(4, entry.getValue());
                pstmt.setInt(5, entry.getValue());
                pstmt.execute();
            }
            
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    /**
     * 处理参数
     * 
     * @param conf
     * @param args
     */
    private void processArgs(Configuration conf, String[] args) {
        String date = null;
        for (int i = 0; i < args.length; i++) {
            if ("-d".equals(args[i])) {
                if (i + 1 < args.length) {
                    date = args[++i];
                    break;
                }
            }
        }

        // 要求date格式为: yyyy-MM-dd
        if (StringUtils.isBlank(date) || !TimeUtil.isValidateRunningDate(date)) {
            // date是一个无效时间数据
            date = TimeUtil.getYesterday(); // 默认时间是昨天
        }
        conf.set(GlobalConstants.RUNNING_DATE_PARAMES, date);
    }

    /**
     * 初始化scan集合
     * 
     * @param job
     * @return
     */
    private List<Scan> initScans(Job job) {
        // 时间戳+....
        Configuration conf = job.getConfiguration();
        // 获取运行时间: yyyy-MM-dd
        String date = conf.get(GlobalConstants.RUNNING_DATE_PARAMES);
        long startDate = TimeUtil.parseString2Long(date);
        long endDate = startDate + GlobalConstants.DAY_OF_MILLISECONDS;

        Scan scan = new Scan();
        // 定义hbase扫描的开始rowkey和结束rowkey
        scan.setStartRow(Bytes.toBytes("" + startDate));
        scan.setStopRow(Bytes.toBytes("" + endDate));

        FilterList filterList = new FilterList();
        // 定义mapper中需要获取的列名
        String[] columns = new String[] { EventLogConstants.LOG_COLUMN_NAME_MEMBER_ID, // 会员id
                EventLogConstants.LOG_COLUMN_NAME_SERVER_TIME, // 服务器时间
                EventLogConstants.LOG_COLUMN_NAME_PLATFORM, // 平台名称
                EventLogConstants.LOG_COLUMN_NAME_BROWSER_NAME, // 浏览器名称
                EventLogConstants.LOG_COLUMN_NAME_BROWSER_VERSION // 浏览器版本信息
        };
        filterList.addFilter(this.getColumnFilter(columns));

        scan.setAttribute(Scan.SCAN_ATTRIBUTES_TABLE_NAME, Bytes.toBytes(EventLogConstants.HBASE_NAME_EVENT_LOGS));
        scan.setFilter(filterList);
        return Lists.newArrayList(scan);
    }

    /**
     * 获取这个列名过滤的column
     * 
     * @param columns
     * @return
     */
    private Filter getColumnFilter(String[] columns) {
        int length = columns.length;
        byte[][] filter = new byte[length][];
        for (int i = 0; i < length; i++) {
            filter[i] = Bytes.toBytes(columns[i]);
        }
        return new MultipleColumnPrefixFilter(filter);
    }
}
