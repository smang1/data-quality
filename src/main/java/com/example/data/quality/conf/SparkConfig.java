package com.example.data.quality.conf;

import lombok.Data;
import org.apache.spark.sql.SparkSession;
import org.springframework.context.annotation.Configuration;

@Data
@Configuration
public class SparkConfig {

    private SparkSession spark;

    public SparkConfig() {
        this.spark = SparkSession.builder()
                .appName("Spark Data Quality with Deequ")
                .master("local")
                .getOrCreate();
    }
}
