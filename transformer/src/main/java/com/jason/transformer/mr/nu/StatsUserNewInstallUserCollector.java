package com.jason.transformer.mr.nu;

import java.io.IOException;
import java.sql.PreparedStatement;
import java.sql.SQLException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;

import com.jason.common.GlobalConstants;
import com.jason.transformer.model.dim.StatsUserDimension;
import com.jason.transformer.model.dim.base.BaseDimension;
import com.jason.transformer.model.value.BaseStatsValueWritable;
import com.jason.transformer.model.value.reduce.MapWritableValue;
import com.jason.transformer.mr.IOutputCollector;
import com.jason.transformer.service.IDimensionConverter;

//具体配合output进行sql输出的类
public class StatsUserNewInstallUserCollector implements IOutputCollector {

    @Override
    public void collect(Configuration conf, BaseDimension key, BaseStatsValueWritable value, PreparedStatement pstmt, IDimensionConverter converter) throws SQLException, IOException {
        StatsUserDimension statsUserDimension = (StatsUserDimension) key;  //输出key
        MapWritableValue mapWritableValue = (MapWritableValue) value;  //输出value
        IntWritable newInstallUsers = (IntWritable) mapWritableValue.getValue().get(new IntWritable(-1)); //输出新增用户

        int i = 0;
        pstmt.setInt(++i, converter.getDimensionIdByValue(statsUserDimension.getStatsCommon().getPlatform()));
        pstmt.setInt(++i, converter.getDimensionIdByValue(statsUserDimension.getStatsCommon().getDate()));
        pstmt.setInt(++i, newInstallUsers.get());
        pstmt.setString(++i, conf.get(GlobalConstants.RUNNING_DATE_PARAMES));
        pstmt.setInt(++i, newInstallUsers.get());
        pstmt.addBatch();
    }

}
