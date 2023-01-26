package com.example.data.quality.runner.impl;

import com.amazon.deequ.analyzers.Analyzer;
import com.amazon.deequ.analyzers.ApproxCountDistinct;
import com.amazon.deequ.analyzers.Completeness;
import com.amazon.deequ.analyzers.runners.AnalysisRunner;
import com.amazon.deequ.analyzers.runners.AnalyzerContext;
import com.amazon.deequ.metrics.Metric;
import com.example.data.quality.conf.Store;
import com.example.data.quality.conf.SparkConfig;
import com.example.data.quality.runner.DqRunner;
import com.example.data.quality.utils.JavaToScalaUtils;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.springframework.stereotype.Component;
import scala.Option;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static scala.collection.JavaConverters.asScalaBuffer;

@Component
public class AnalyzerRunner implements DqRunner {
    private final SparkConfig sparkConfig;
    private final Store store;

    public AnalyzerRunner(SparkConfig sparkConfig, Store store) {
        this.sparkConfig = sparkConfig;
        this.store = store;
    }

    @Override
    public void run(String file) {
        SparkSession ss = sparkConfig.getSpark();

        Dataset<Row> input = ss.read()
                .option("delimiter", ";")
                .option("header", "true")
                .csv(file);
        input.printSchema();
        input.show(false);
        List<String> allFields = store.getAllFields();

        List<Analyzer<?, Metric<?>>> analyzers = new ArrayList<>();

        for(String field: allFields){
            analyzers.add((Analyzer) new Completeness(field, Option.empty()));//completeness
            analyzers.add((Analyzer) new ApproxCountDistinct(field, Option.empty())); //ApproxCountDistinct
        }

        /*List<String> streetCountDistinctKey = Arrays.asList("Street");
          .addAnalyzer((Analyzer) new Size(Option.empty()))
                .addAnalyzer((Analyzer) new Completeness("*", Option.empty()))
          .addAnalyzer((Analyzer) new DataType("Street", Option.empty()))
          .addAnalyzer((Analyzer) new CountDistinct(toSeqString(streetCountDistinctKey)))
          .addAnalyzer((Analyzer) new Completeness("City", Option.empty()))*/

        AnalyzerContext analysisResult = AnalysisRunner
                .onData(input)
                .addAnalyzers(JavaToScalaUtils.toSeq(analyzers))
                .run();


        Dataset<Row> result = AnalyzerContext.successMetricsAsDataFrame(ss, analysisResult, asScalaBuffer(Collections.emptyList()));

        result.sort("instance","name").show(100, false);


    }
}
