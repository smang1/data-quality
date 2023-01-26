package com.example.data.quality.model;

import lombok.Builder;
import lombok.Data;

import java.util.Map;

@Builder
@Data
public class DataProfile {
    private String columnName;
    public double completeness;
    public long approximateNumDistinctValues;
    public String dataType;
    public boolean isDataTypeInferred;
    public Map<String, String> dataTypeCounts;
}
