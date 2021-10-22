package config;

public class Constants {
    public static class Storage {
        public static final String HOTELS_URL = "abfss://m06sparkbasics@bd201stacc.dfs.core.windows.net/hotels";
        public static final String WEATHER_URL = "abfss://m06sparkbasics@bd201stacc.dfs.core.windows.net/weather";
        public static final String HOTELS_WEATHER_URL = "abfss://data@sttislenkoaawesteurope.dfs.core.windows.net/hotels_weather";
    }

    public static class Security {
        public static final String STORAGE_KEY_VALUE = "you key to blob storage";
    }
}
