package services.api;

public interface LocationService {

    String getAddress(double latitude, double longitude, String language);

    double[] getCoordinates(String address, String countryCode);

    double getLatitude(String address);

    double getLongitude(String address);
}
