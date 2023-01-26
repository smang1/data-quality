package com.example.data.quality.conf;

import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Configuration;

import java.util.List;

@Data
@Configuration
@ConfigurationProperties(prefix="store")
public class Store {
    private List<String> allFields;
    private List<String> baseFields;
}
