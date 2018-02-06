package com.jason.transformer.mr.pv;

import java.util.List;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.MultipleColumnPrefixFilter;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;

import com.jason.common.EventLogConstants;
import com.jason.common.GlobalConstants;
import com.jason.transformer.model.dim.StatsUserDimension;
import com.jason.transformer.model.value.reduce.MapWritableValue;
import com.jason.transformer.mr.TransformerOutputFormat;
import com.jason.util.TimeUtil;
import com.google.common.collect.Lists;

/**
 * 计算website的pv值的mapreducer入口类<br/>
 * 从hbase中获取platform、servertime、browsername、browserversion以及url，
 * 并且只获取pageview事件的数据<br/>
 * 将计算得到的pv值保存到stats_device_browser表中。
 * 
 * @author jason
 *
 */
public class PageViewRunner implements Tool {
    private static final Logger logger = Logger.getLogger(PageViewRunner.class);
    private Configuration conf;

    public static void main(String[] args) {
        try {
            ToolRunner.run(new Configuration(), new PageViewRunner(), args);
        } catch (Exception e) {
            logger.error("计算pv任务出现异常", e);
            throw new RuntimeException("运行job异常", e);
        }
    }

    @Override
    public void setConf(Configuration conf) {
        // 添加自定义configuration配置文件
        conf.addResource("query-mapping.xml");
        conf.addResource("output-collector.xml");
        conf.addResource("transformer-env.xml");
        // 使用hbase提供的类进行加载hbase相关配置
        this.conf = HBaseConfiguration.create(conf);
    }

    @Override
    public Configuration getConf() {
        return this.conf;
    }

    @Override
    public int run(String[] args) throws Exception {
        Configuration conf = this.getConf();
        // 参数处理
        this.processArgs(conf, args);

        // job创建
        Job job = Job.getInstance(conf, "website_pageview");
        // 设置相关参数
        job.setJarByClass(PageViewRunner.class);
        // 设置mapper相关参数, 最后一个参数，本地运行指定为false，线上运行指定为true。
        TableMapReduceUtil.initTableMapperJob(this.initScans(job), PageViewMapper.class, StatsUserDimension.class, NullWritable.class, job, false);
        // 设置reducer相关参数
        job.setReducerClass(PageViewReducer.class);
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
        // 只需要pageview事件
        filterList.addFilter(new SingleColumnValueFilter(PageViewMapper.family, Bytes.toBytes(EventLogConstants.LOG_COLUMN_NAME_EVENT_NAME), CompareOp.EQUAL, Bytes.toBytes(EventLogConstants.EventEnum.PAGEVIEW.alias)));
        // 定义mapper中需要获取的列名
        String[] columns = new String[] { EventLogConstants.LOG_COLUMN_NAME_EVENT_NAME, // 获取事件名称
                EventLogConstants.LOG_COLUMN_NAME_CURRENT_URL, // 当前url
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
