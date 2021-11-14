package config;

import org.apache.spark.SparkConf;
import services.impl.SecurityServiceImpl;

import java.util.Map;

public class SparkConfig {

    public static SparkConf getSparkConfigUsing(SecurityServiceImpl securityService) {
        SparkConf sparkConf = new SparkConf()
                .setAppName("Driver")
                .setMaster("local[*]");
        for (Map.Entry<String, String> entry: securityService.getInputStorageCredentials().entrySet()) {
            sparkConf.set(entry.getKey(), entry.getValue());
        }
        return sparkConf;
    }
}
