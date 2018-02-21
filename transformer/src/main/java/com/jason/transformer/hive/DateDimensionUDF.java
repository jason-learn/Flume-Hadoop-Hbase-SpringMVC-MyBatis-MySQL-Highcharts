package com.jason.transformer.hive;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;

import com.jason.common.DateEnum;
import com.jason.transformer.model.dim.base.DateDimension;
import com.jason.transformer.service.rpc.IDimensionConverter;
import com.jason.transformer.service.rpc.client.DimensionConverterClient;
import com.jason.util.TimeUtil;

/**
 * 操作日期dimension 相关的udf
 * 
 * @author jason
 *
 */
public class DateDimensionUDF extends UDF {
    private IDimensionConverter converter = null;

    public DateDimensionUDF() {
        try {
            this.converter = DimensionConverterClient.createDimensionConverter(new Configuration());
        } catch (IOException e) {
            throw new RuntimeException("创建converter异常");
        }

        // 添加一个钩子进行关闭操作
        Runtime.getRuntime().addShutdownHook(new Thread(new Runnable() {

            @Override
            public void run() {
                try {
                    DimensionConverterClient.stopDimensionConverterProxy(converter);
                } catch (Throwable e) {
                    // nothing
                }
            }
        }));
    }

    /**
     * 根据给定的日期（格式为:yyyy-MM-dd）至返回id
     * 
     * @param day
     * @return
     */
    public IntWritable evaluate(Text day) {
        DateDimension dimension = DateDimension.buildDate(TimeUtil.parseString2Long(day.toString()), DateEnum.DAY);
        try {
            int id = this.converter.getDimensionIdByValue(dimension);
            return new IntWritable(id);
        } catch (IOException e) {
            throw new RuntimeException("获取id异常");
        }
    }
}
