package config;

public class Constants {
    public static class Storage {
        public static final String HOTELS_URL = "abfss://m06sparkbasics@bd201stacc.dfs.core.windows.net/hotels";
        public static final String WEATHER_URL = "abfss://m06sparkbasics@bd201stacc.dfs.core.windows.net/weather";
        public static final String HOTELS_WEATHER_URL = "abfss://data@sttislenkoawesteurope.dfs.core.windows.net/hotels_weather";
        //set the name of your azure key vault in environment variable or just hardcode it if you wish
        public static final String SECRET_VAULT_URL = "https://" + System.getenv("KEY_VAULT_NAME") + ".vault.azure.net";
    }

    public static class Security {
        public static final String OUTPUT_STORAGE_SECRET_NAME="sttislenkoawesteurope";
        public static final String INPUT_STORAGE_OAUTH_SECRET_NAME = "m06sparkbasics";
        public static final String JOPENCAGE_API_KEY_NAME = "jopencage-api-key";
    }
}
