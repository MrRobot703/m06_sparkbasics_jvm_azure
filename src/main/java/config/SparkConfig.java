package config;

import org.apache.spark.SparkConf;

public class SparkConfig {

    public static SparkConf getSparkConfig() {
        return new SparkConf()
                .setAppName("Driver")
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
    }
}
