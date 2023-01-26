package com.example.data.quality;

import com.example.data.quality.runner.impl.AnalyzerRunner;
import com.example.data.quality.runner.impl.ConstraintSugRunner;
import com.example.data.quality.runner.impl.ProfileRunner;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.ComponentScan;

/**
 * Hello world!
 */
@Slf4j
@SpringBootApplication
@ComponentScan(basePackages = "com.example.data.quality")
public class App implements CommandLineRunner {
    private static String DATA_FILE = "D:\\Sodexo\\docs\\data\\source_application\\Velvet_output\\allcol_SodexoFsqGoogleCzechia.csv";
  //  private DqRunner dqRunner;
    private final AnalyzerRunner analyzerRunner;
    private final ProfileRunner profileRunner;
    private  final ConstraintSugRunner constraintSugRunner;

    public App(AnalyzerRunner analyzerRunner, ProfileRunner profileRunner, ConstraintSugRunner constraintSugRunner) {
        this.analyzerRunner = analyzerRunner;
        this.profileRunner = profileRunner;
        this.constraintSugRunner = constraintSugRunner;
    }

    public static void main(String[] args)  {
        log.info("Starting Data Quality app");
        SpringApplication.run(App.class, args);

    }


    @Override
    public void run(String... args) throws Exception {
        log.info("Running Data Quality app");
        //dqRunner.run(DATA_FILE);
        //profileRunner.run(DATA_FILE);
        constraintSugRunner.run(DATA_FILE);
    }
}
