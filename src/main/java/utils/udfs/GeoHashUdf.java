package utils.udfs;

import ch.hsr.geohash.GeoHash;
import org.apache.spark.sql.api.java.UDF2;

public class GeoHashUdf implements UDF2<Double, Double, String> {

    @Override
    public String call(Double latitude, Double longitude) throws Exception {
        if (latitude == null || longitude == null) return null;

        return hash(latitude, longitude);
    }

    private String hash(double latitude, double longitude, int precision) {
        return GeoHash.withCharacterPrecision(latitude, longitude, precision).toBase32();
    }

    private String hash(double latitude, double longitude) {
        return hash(latitude, longitude, 4);
    }
}
