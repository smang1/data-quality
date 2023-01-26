package com.example.data.quality.runner.impl;

import com.amazon.deequ.profiles.ColumnProfile;
import com.amazon.deequ.profiles.ColumnProfilerRunner;
import com.amazon.deequ.profiles.ColumnProfiles;
import com.example.data.quality.conf.SparkConfig;
import com.example.data.quality.model.DataProfile;
import com.example.data.quality.runner.DqRunner;
import com.example.data.quality.utils.JavaToScalaUtils;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.springframework.stereotype.Component;
import scala.collection.JavaConversions;
import scala.collection.JavaConverters;
import scala.collection.mutable.Buffer;

import java.util.*;

import static org.apache.spark.sql.functions.col;

@Component
public class ProfileRunner implements DqRunner {
    private final SparkConfig sparkConfig;

    public ProfileRunner(SparkConfig sparkConfig) {
        this.sparkConfig = sparkConfig;
    }

    @Override
    public void run(String file) {
        SparkSession ss = sparkConfig.getSpark();

        Dataset<Row> input = ss.read()
                .option("delimiter", ";")
                .option("header", "true")
                .csv(file);
        input.persist();
        input.printSchema();
        input.show(false);

        ColumnProfilerRunner columnProfilerRunner = new ColumnProfilerRunner();
        ColumnProfiles profiles = columnProfilerRunner.onData(input).run();

        java.util.Map<String, ColumnProfile> profileMap = JavaConverters.mapAsJavaMapConverter(profiles.profiles()).asJava();

        List<DataProfile> dataProfileList = new ArrayList<>();
        profileMap.forEach((k, v) -> {
            Map<String, Object> stringObjectMap = JavaToScalaUtils.toJavaMap(v.typeCounts());

            Map<String, String> typeCounts = new HashMap<>();
            stringObjectMap.forEach((type, count) -> typeCounts.put(type, count.toString()));

            DataProfile currColumnProfile = buildProfile(k, v, typeCounts);
            dataProfileList.add(currColumnProfile);

        });

        List<Column> columns = Arrays.asList(col("columnName"), col("completeness"), col("approximateNumDistinctValues"), col("dataType"), col("dataTypeInferred"), col("dataTypeCounts"));
        Buffer<Column> columnsBuffer = JavaConversions.asScalaBuffer(columns);

        Dataset<Row> dataProfileDs = ss.createDataFrame(dataProfileList, DataProfile.class);
        dataProfileDs = dataProfileDs.select(columnsBuffer);

        dataProfileDs.printSchema();
        System.out.println("Number of input rows: " + input.count());
        dataProfileDs.show(100, false);

        profileMap.forEach((k, v) -> {
            System.out.println("Column " + k);
            System.out.println(v);
        });

        input.unpersist();
    }

    private static DataProfile buildProfile(String k, ColumnProfile v, Map<String, String> typeCounts) {
        return DataProfile.builder()
                .columnName(k.toString())
                .dataType(v.dataType().toString())
                .isDataTypeInferred(v.isDataTypeInferred())
                .completeness(v.completeness())
                .approximateNumDistinctValues(v.approximateNumDistinctValues())
                .dataTypeCounts(typeCounts)
                .build();
    }


}
