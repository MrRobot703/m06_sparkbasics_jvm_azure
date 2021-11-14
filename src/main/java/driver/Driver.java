package driver;

import config.Constants;
import config.IOConfig;
import config.SparkConfig;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import services.impl.SecurityServiceImpl;
import utils.udfs.GeoHashUdf;
import utils.udfs.CorrectCoordinatesUdf;

import static org.apache.spark.sql.functions.*;

//The driver of the spark application
public class Driver {

    private final SparkSession sparkSession;

    private final SecurityServiceImpl securityService = new SecurityServiceImpl();

    public static void main(String[] args) {
        Driver driver = new Driver();
        driver.start();
    }

    /*
    * Bootstrapping steps of the application:
    * initializing spark session and setting user defined functions
    * */
    public Driver() {
        sparkSession = SparkSession.builder()
                .config(SparkConfig.getSparkConfigUsing(securityService))
                .getOrCreate();

        sparkSession.udf().register("getCorrectCoordinates",
                new CorrectCoordinatesUdf(),
                DataTypes.createArrayType(DataTypes.DoubleType)
        );
        sparkSession.udf().register("geoHash",
                new GeoHashUdf(4),
                DataTypes.StringType
        );
    }

    private void start() {

        /*
        *                          Extracting stage.
        * Retrieving the data from blob storage (with according fine-tuning of
        * parquet format retrieval)
        * */
        Dataset<Row> hotelsDF= getCSVData(Constants.Storage.HOTELS_URL);
        Dataset<Row> weatherDF = getParquetData(Constants.Storage.WEATHER_URL);

        hotelsDF.show(5);
        weatherDF.show(5);

        /*
        *                          Cleansing stage.
        * Filtering out the nulls of latitude and longitude in initial hotels' dataset
        * by combining the part of the dataset without nulls (complement of the
        * part with nulls) with new part where latitude and longitude are populated
        * by location service (jOpenCage).
        *                          Transformation stage.
        * Also computing 4-character geohash on aforementioned columns as an additional
        * column.
        * */
        hotelsDF = hotelsDF.filter("Latitude IS NOT NULL AND Longitude IS NOT NULL")
                .union(hotelsWithCorrectLatitudeAndLongitude(hotelsDF))
                .withColumn("geoHash",
                        callUDF("geoHash",
                                col("Latitude").cast(DataTypes.DoubleType),
                                col("Longitude").cast(DataTypes.DoubleType)
                        )
                );

        /*
        * Computing 4-character geohash on weather dataset based on latitude and longitude
        * as well and adding this value to new column.
         * */
        weatherDF = weatherDF.withColumn("geoHash",
                callUDF("geoHash", col("lat"), col("lng"))
        );

        /*
        * Left joining hotels by weather on previously computed geohash columns
        * */
        Dataset<Row> hotelsWeatherDataframe = hotelsDF
                .join(
                        weatherDF,
                        hotelsDF.col("geoHash").equalTo(weatherDF.col("geoHash")),
                        "left"
                )
                .drop("geoHash");

        hotelsWeatherDataframe.show(10);

        /*
        *                          Loading stage.
        * Outputting the result to a blob storage (azure data lake gen 2)
        * */
        hotelsWeatherDataframe.repartition(col("year"), col("month"), col("day"))
                .write()
                .mode("overwrite")
                .options(IOConfig.getOptimalParquetIOConfig())
                .options(IOConfig.getOptimalObjectStoreIOConfig())
                .options(securityService.getOutPutStorageCredentials())
                .partitionBy("year", "month", "day")
                .format("parquet")
                .save(Constants.Storage.HOTELS_WEATHER_URL);

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
                .options(IOConfig.getOptimalParquetIOConfig())
                .load(url);
    }
}
