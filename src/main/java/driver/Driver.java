package driver;

import org.apache.spark.SparkConf;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import utils.udfs.GeoHashUdf;
import utils.udfs.LatitudeLongitudeUdf;

import static org.apache.spark.sql.functions.*;

public class Driver {

    private final SparkSession sparkSession;

    private static final String HOTELS_URL = "abfss://m06sparkbasics@bd201stacc.dfs.core.windows.net/hotels";
    private static final String WEATHER_URL = "abfss://m06sparkbasics@bd201stacc.dfs.core.windows.net/weather";

    public static void main(String[] args) {
        Driver driver = new Driver();
        driver.start();
    }

    public Driver() {
        SparkConf sparkConf = new SparkConf()
                .setAppName("Dummy Driver")
                .setMaster("local[*]")
                .set("fs.azure.account.auth.type.bd201stacc.dfs.core.windows.net",
                        "OAuth"
                )
                .set("fs.azure.account.oauth.provider.type.bd201stacc.dfs.core.windows.net",
                        "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider"
                )
                .set("fs.azure.account.oauth2.client.id.bd201stacc.dfs.core.windows.net",
                        "f3905ff9-16d4-43ac-9011-842b661d556d"
                )
                .set("fs.azure.account.oauth2.client.secret.bd201stacc.dfs.core.windows.net",
                        "mAwIU~M4~xMYHi4YX_uT8qQ.ta2.LTYZxT"
                )
                .set("fs.azure.account.oauth2.client.endpoint.bd201stacc.dfs.core.windows.net",
                        "https://login.microsoftonline.com/b41b72d0-4e9f-4c26-8a69-f949f367c91d/oauth2/token"
                );
        sparkSession = SparkSession.builder()
                .config(sparkConf)
                .getOrCreate();

        sparkSession.udf().register("getCorrectCoordinates",
                new LatitudeLongitudeUdf(),
                DataTypes.createArrayType(DataTypes.DoubleType)
        );
        sparkSession.udf().register("geoHash",
                new GeoHashUdf(),
                DataTypes.StringType
        );
    }

    private void start() {
        Dataset<Row> hotelsDF= getCSVData(HOTELS_URL);
        Dataset<Row> weatherDF = getParquetData(WEATHER_URL);

        hotelsDF = hotelsDF.filter("Latitude IS NOT NULL AND Longitude IS NOT NULL")
                .union(hotelsWithCorrectLatitudeAndLongitude(hotelsDF))
                .withColumn("geoHash",
                        callUDF("geoHash",
                                col("Latitude").cast(DataTypes.DoubleType),
                                col("Longitude").cast(DataTypes.DoubleType)
                        )
                );

        weatherDF = weatherDF.withColumn("geoHash",
                callUDF("geoHash", col("lat"), col("lng"))
        );

        Dataset<Row> hotelsWeatherDataframe = hotelsDF
                .join(weatherDF, hotelsDF.col("geoHash").equalTo(weatherDF.col("geoHash")), "left")
                .drop("geoHash");

        hotelsWeatherDataframe.show(5);
        hotelsWeatherDataframe.printSchema();

        this.sparkSession.stop();
    }

    private Dataset<Row> hotelsWithCorrectLatitudeAndLongitude(Dataset<Row> hotelsDF) {
        return hotelsDF.filter("Latitude is NULL or Longitude is NULL")
                .withColumn("result",
                        callUDF("getCorrectCoordinates",
                                col("Country"),
                                col("City"),
                                col("Address")
                        )
                )
                .cache()
                .withColumn("Latitude", element_at(col("result"), 1).as(Encoders.STRING()))
                .withColumn("Longitude", element_at(col("result"), 2).as(Encoders.STRING()))
                .drop("result");
    }

    private Dataset<Row> getCSVData(String url) {
        return this.sparkSession.read()
                .format("csv")
                .option("header", "true")
                .option("inferSchema", "true")
                .load(url);
    }

    private Dataset<Row> getParquetData(String url) {
        return this.sparkSession.read()
                .format("parquet")
                .option("inferSchema", "false")
                .load(url);
    }
}
