package com.jason.transformer.mr.sessions;

import java.util.List;

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

import com.jason.common.EventLogConstants;
import com.jason.common.GlobalConstants;
import com.jason.transformer.model.dim.StatsUserDimension;
import com.jason.transformer.model.value.map.TimeOutputValue;
import com.jason.transformer.model.value.reduce.MapWritableValue;
import com.jason.transformer.mr.TransformerOutputFormat;
import com.jason.util.TimeUtil;
import com.google.common.collect.Lists;

/**
 * 计算会话信息的入口执行类
 * 
 * @author jason
 *
 */
public class SessionsRunner implements Tool {
    private static final Logger logger = Logger.getLogger(SessionsRunner.class);
    private Configuration conf = null;

    public static void main(String[] args) {
        try {
            ToolRunner.run(new Configuration(), new SessionsRunner(), args);
        } catch (Exception e) {
            logger.error("运行计算session的mapreduce任务出现异常", e);
            throw new RuntimeException("执行任务失败", e);
        }
    }

    @Override
    public void setConf(Configuration conf) {
        // 添加自定义的配置文件
        conf.addResource("transformer-env.xml");
        conf.addResource("query-mapping.xml");
        conf.addResource("output-collector.xml");
        // 创建hbase相关的config对象(包含hbase配置文件)
        // hbase创建config的时候，会将指定参数的configuration所有的内容加载到内存中。
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
        Job job = Job.getInstance(conf, "sessions");
        // 设置相关参数
        job.setJarByClass(SessionsRunner.class);
        // 设置mapper相关参数, 最后一个参数，本地运行指定为false，线上运行指定为true。
        TableMapReduceUtil.initTableMapperJob(this.initScans(job), SessionsMapper.class, StatsUserDimension.class, TimeOutputValue.class, job, false);
        // 设置reducer相关参数
        job.setReducerClass(SessionsReducer.class);
        job.setOutputKeyClass(StatsUserDimension.class);
        job.setOutputValueClass(MapWritableValue.class);
        // 设置outputformat
        job.setOutputFormatClass(TransformerOutputFormat.class);
        return job.waitForCompletion(true) ? 0 : -1;
    }

    /**
     * 处理参数
     * 
     * @param conf
     * @param args
     */
    protected void processArgs(Configuration conf, String[] args) {
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
        String[] columns = new String[] { EventLogConstants.LOG_COLUMN_NAME_SESSION_ID, // 会话id
                EventLogConstants.LOG_COLUMN_NAME_SERVER_TIME, // 服务器时间
                EventLogConstants.LOG_COLUMN_NAME_PLATFORM, // 平台名称
                EventLogConstants.LOG_COLUMN_NAME_BROWSER_NAME, // 浏览器名称
                EventLogConstants.LOG_COLUMN_NAME_BROWSER_VERSION // 浏览器版本号
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
