package config;

import java.util.HashMap;
import java.util.Map;

public class Security {

    public static Map<String, String> getOutPutStorageCredentials() {
        HashMap<String, String> credentials = new HashMap<>();
        credentials.put("fs.azure.account.key", Constants.Security.STORAGE_KEY_VALUE);
        return credentials;
    }
}
