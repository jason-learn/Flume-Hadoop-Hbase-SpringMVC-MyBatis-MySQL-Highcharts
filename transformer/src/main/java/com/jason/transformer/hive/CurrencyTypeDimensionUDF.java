package com.jason.transformer.hive;

import java.io.IOException;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.ql.exec.UDF;

import com.jason.common.GlobalConstants;
import com.jason.transformer.model.dim.base.CurrencyTypeDimension;
import com.jason.transformer.service.rpc.IDimensionConverter;
import com.jason.transformer.service.rpc.client.DimensionConverterClient;

/**
 * 订单支付货币类型dimension操作udf
 * 
 * @author jason
 *
 */
public class CurrencyTypeDimensionUDF extends UDF {
    private IDimensionConverter converter = null;

    public CurrencyTypeDimensionUDF() {
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
     * 根据给定的currency名称，返回对应的id值
     * 
     * @param currencyName
     * @return
     */
    public int evaluate(String currencyName) {
        currencyName = StringUtils.isBlank(currencyName) ? GlobalConstants.DEFAULT_VALUE : currencyName.trim();
        CurrencyTypeDimension dimension = new CurrencyTypeDimension(currencyName);
        try {
            return this.converter.getDimensionIdByValue(dimension);
        } catch (IOException e) {
            throw new RuntimeException("获取id异常");
        }
    }
}
