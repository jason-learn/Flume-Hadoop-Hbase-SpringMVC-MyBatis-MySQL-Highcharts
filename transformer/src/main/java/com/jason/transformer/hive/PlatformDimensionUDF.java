package com.jason.transformer.hive;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;

import com.jason.transformer.model.dim.base.PlatformDimension;
import com.jason.transformer.service.rpc.IDimensionConverter;
import com.jason.transformer.service.rpc.client.DimensionConverterClient;

/**
 * 操作平台dimension 相关的udf
 * 
 * @author jason
 *
 */
public class PlatformDimensionUDF extends UDF {
    private IDimensionConverter converter = null;

    public PlatformDimensionUDF() {
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
     * 根据给定的平台名称返回对应的id
     * 
     * @param platformName
     * @return
     */
    public IntWritable evaluate(Text platformName) {
        PlatformDimension dimension = new PlatformDimension(platformName.toString());
        try {
            int id = this.converter.getDimensionIdByValue(dimension);
            return new IntWritable(id);
        } catch (IOException e) {
            throw new RuntimeException("获取id异常");
        }
    }
}
