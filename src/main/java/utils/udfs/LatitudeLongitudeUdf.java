package utils.udfs;

import org.apache.spark.sql.api.java.UDF3;
import services.api.LocationService;
import services.impl.LocationServiceImpl;

public class LatitudeLongitudeUdf implements UDF3<String, String, String, Double[]> {

    private final LocationService locationService = new LocationServiceImpl();

    @Override
    public Double[] call(String countryCode, String city, String address) throws Exception {
        String fullAddress = address + ", " + city;
        double[] coordinates = locationService.getCoordinates(fullAddress, countryCode);
        return new Double[]{coordinates[0], coordinates[1]};
    }
}
