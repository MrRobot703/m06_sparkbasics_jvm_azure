package services.impl;

import com.azure.identity.DefaultAzureCredentialBuilder;
import com.azure.security.keyvault.secrets.SecretClient;
import com.azure.security.keyvault.secrets.SecretClientBuilder;
import com.azure.security.keyvault.secrets.models.KeyVaultSecret;
import config.Constants;
import services.api.SecurityService;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

public class SecurityServiceImpl implements SecurityService {

    private final SecretClient secretClient;

    public SecurityServiceImpl() {
        secretClient = new SecretClientBuilder()
                .vaultUrl(Constants.Storage.SECRET_VAULT_URL)
                .credential(new DefaultAzureCredentialBuilder().build())
                .buildClient();
    }

    @Override
    public String getSecret(String name) {
        return Optional.of(secretClient.getSecret(name)).map(KeyVaultSecret::getValue).get();
    }

    public Map<String, String> getOutPutStorageCredentials() {
        HashMap<String, String> credentials = new HashMap<>();
        String key = getSecret(Constants.Security.OUTPUT_STORAGE_SECRET_NAME);
        credentials.put("fs.azure.account.key", key);
        return credentials;
    }

    public Map<String, String> getInputStorageCredentials() {
        HashMap<String, String> oAuthCredentials = new HashMap<>(5);
        oAuthCredentials.put("fs.azure.account.auth.type.bd201stacc.dfs.core.windows.net", "OAuth");
        oAuthCredentials.put("fs.azure.account.oauth.provider.type.bd201stacc.dfs.core.windows.net",
                "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider");
        oAuthCredentials.put("fs.azure.account.oauth2.client.id.bd201stacc.dfs.core.windows.net",
                "f3905ff9-16d4-43ac-9011-842b661d556d");
        oAuthCredentials.put("fs.azure.account.oauth2.client.secret.bd201stacc.dfs.core.windows.net",
                getSecret(Constants.Security.INPUT_STORAGE_OAUTH_SECRET_NAME));
        oAuthCredentials.put("fs.azure.account.oauth2.client.endpoint.bd201stacc.dfs.core.windows.net",
                "https://login.microsoftonline.com/b41b72d0-4e9f-4c26-8a69-f949f367c91d/oauth2/token");
        return oAuthCredentials;
    }
}
