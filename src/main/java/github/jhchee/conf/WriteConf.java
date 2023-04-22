package github.jhchee.conf;

import org.apache.hudi.DataSourceWriteOptions;

import java.util.HashMap;
import java.util.Map;

public class WriteConf {

    public static Map<String, String> getHiveSyncOptions(String database, String table) {
        Map<String, String> options = new HashMap<>();
        options.put(DataSourceWriteOptions.HIVE_SYNC_ENABLED_OPT_KEY(), "true");
        options.put(DataSourceWriteOptions.HIVE_TABLE().key(), table);
        options.put(DataSourceWriteOptions.HIVE_DATABASE().key(), database);
        options.put("hoodie.metadata.enable", "false"); // for minio docker only
        options.put(DataSourceWriteOptions.HIVE_USE_JDBC().key(), "false");
        options.put(DataSourceWriteOptions.METASTORE_URIS().key(), "thrift://localhost:9083");
        options.put(DataSourceWriteOptions.HIVE_SYNC_MODE().key(), "hms");
        return options;
    }
}
