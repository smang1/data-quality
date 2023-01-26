package com.example.data.quality.runner.impl;

import com.amazon.deequ.suggestions.ConstraintSuggestion;
import com.amazon.deequ.suggestions.ConstraintSuggestionResult;
import com.amazon.deequ.suggestions.ConstraintSuggestionRunner;
import com.amazon.deequ.suggestions.Rules;
import com.example.data.quality.conf.SparkConfig;
import com.example.data.quality.conf.Store;
import com.example.data.quality.runner.DqRunner;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.springframework.stereotype.Component;
import scala.collection.JavaConverters;
import scala.collection.Seq;

import java.util.List;
import java.util.Map;

@Component
public class ConstraintSugRunner implements DqRunner {

    private final SparkConfig sparkConfig;
    private final Store store;

    public ConstraintSugRunner(SparkConfig sparkConfig, Store store) {
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

        ConstraintSuggestionResult constraintsResult = new ConstraintSuggestionRunner().onData(input).addConstraintRules(Rules.DEFAULT()).run();
        Map<String, Seq<ConstraintSuggestion>> constraintResultsMap = JavaConverters.mapAsJavaMapConverter(constraintsResult.constraintSuggestions()).asJava();

        constraintResultsMap.forEach((k, v) -> {
                    List<ConstraintSuggestion> constraintSuggestions = JavaConverters.seqAsJavaList(v);
                    constraintSuggestions.forEach(constraintSuggestion -> System.out.println(String.format("Key: %s, Description: %s", constraintSuggestion.columnName(), constraintSuggestion.description(), constraintSuggestion.codeForConstraint())));
                }
            );
    }
}
