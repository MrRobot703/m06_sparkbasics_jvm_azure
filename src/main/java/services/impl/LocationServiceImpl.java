package services.impl;

import com.byteowls.jopencage.JOpenCageGeocoder;
import com.byteowls.jopencage.model.JOpenCageForwardRequest;
import com.byteowls.jopencage.model.JOpenCageResponse;
import com.byteowls.jopencage.model.JOpenCageReverseRequest;
import services.api.LocationService;

import java.io.Serializable;

/*
* JOpenCage service for mapping between latitude and longitude coordinates and
* text address
* */
public class LocationServiceImpl implements LocationService, Serializable {

    private static final String API_KEY = "c8e728bf813f409f9ac68acf30b99156";

    @Override
    public String getAddress(double latitude, double longitude, String language) {
        return getResponse(latitude, longitude, language)
                .getResults()
                .get(0)
                .getFormatted();
    }

    @Override
    public double[] getCoordinates(String address, String countryCode) {
        JOpenCageResponse response = getResponse(address, countryCode);
        double latitude = response.getFirstPosition().getLat();
        double longitude = response.getFirstPosition().getLng();
        return new double[]{latitude, longitude};
    }

    @Override
    public double getLatitude(String address) {
        return 0;
    }

    @Override
    public double getLongitude(String address) {
        return 0;
    }

    private JOpenCageResponse getResponse(double latitude, double longitude, String language) {
        JOpenCageReverseRequest request = new JOpenCageReverseRequest(latitude, longitude);
        request.setLanguage(language);
        request.setNoDedupe(true);
        request.setLimit(1);
        request.setNoAnnotations(true);
        request.setMinConfidence(3);
        return new JOpenCageGeocoder(API_KEY).reverse(request);
    }

    private JOpenCageResponse getResponse(String address, String countyCode) {
        JOpenCageForwardRequest request = new JOpenCageForwardRequest(address);
        request.setRestrictToCountryCode(countyCode);
        request.setLimit(1);
        return new JOpenCageGeocoder(API_KEY).forward(request);
    }
}
