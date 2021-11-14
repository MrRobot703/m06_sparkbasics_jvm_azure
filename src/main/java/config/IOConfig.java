package config;

import java.util.HashMap;
import java.util.Map;

public class IOConfig {

    /*
    * Parquet config properties for optimization
    * */
    public static Map<String, String> getOptimalObjectStoreIOConfig() {
        HashMap<String, String> config = new HashMap<>();
        config.put("spark.hadoop.mapreduce.fileoutputcommitter.algorithm.version", "2");
        config.put("spark.hadoop.mapreduce.fileoutputcommitter.cleanup-failures.ignored", "true");
        //config.put("spark.speculation", "false");
        //config.put("spark.hadoop.mapreduce.fileoutputcommitter.cleanup.skipped", "true");
        //config.put("spark.hadoop.parquet.overwrite.output.file", "true");
        return config;
    }

    public static Map<String, String> getOptimalParquetIOConfig() {
        HashMap<String, String> config = new HashMap<>();
        config.put("spark.hadoop.parquet.enable.summary-metadata", "false");
        config.put("spark.sql.parquet.mergeSchema", "false");
        config.put("spark.sql.parquet.filterPushdown", "true");
        config.put("spark.sql.hive.metastorePartitionPruning", "true");
        return config;
    }
}
