package github.jhchee;

import org.apache.spark.sql.SparkSession;

public class CreateTable {
    public static void main(String[] args) {
        SparkSession spark = SparkSession.builder()
                                         .appName("Create Hudi Tables.")
                                         .config("spark.sql.warehouse.dir", "s3a://spark/")
                                         .config("hive.metastore.uris", "thrift://localhost:9083")
                                         .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
                                         .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.hudi.catalog.HoodieCatalog")
                                         .config("spark.sql.extensions", "org.apache.spark.sql.hudi.HoodieSparkSessionExtension")
                                         .config("spark.sql.legacy.parquet.nanosAsLong", "true")
                                         .config("hoodie.schema.on.read.enable", "true")
                                         .enableHiveSupport()
                                         .getOrCreate();


        spark.sql("CREATE TABLE target (\n" +
                "  userId STRING,\n" +
                "  name STRING,\n" +
                "  favoriteEsports STRING,\n" +
                "  updatedAt TIMESTAMP\n" +
                ")\n" +
                "USING hudi\n" +
                "OPTIONS (\n" +
                "  type = 'cow',\n" +
                "  primaryKey = 'userId',\n" +
                "  preCombineField = 'updatedAt',\n" +
                "  hoodie.datasource.hive_sync.enable = 'true',\n" +
                "  hoodie.datasource.hive_sync.table = 'target',\n" +
                "  hoodie.metadata.enable = 'false',\n" +
                "  hoodie.datasource.hive_sync.use_jdbc = 'false',\n" +
                "  hoodie.datasource.hive_sync.metastore.uris = 'thrift://localhost:9083',\n" +
                "  hoodie.datasource.hive_sync.mode = 'hms'\n" +
                ")\n" +
                "LOCATION 's3a://spark/target'");

        spark.sql("set hoodie.schema.on.read.enable=true");
        spark.sql("CREATE TABLE target_complex (\n" +
                "  userId STRING,\n" +
                "  nested STRUCT<id: String>,\n" +
                "  updatedAt TIMESTAMP\n" +
                ")\n" +
                "USING hudi\n" +
                "OPTIONS (\n" +
                "  type = 'cow',\n" +
                "  primaryKey = 'userId',\n" +
                "  preCombineField = 'updatedAt',\n" +
                "  hoodie.datasource.hive_sync.enable = 'true',\n" +
                "  hoodie.datasource.hive_sync.table = 'target_complex',\n" +
                "  hoodie.metadata.enable = 'false',\n" +
                "  hoodie.datasource.hive_sync.use_jdbc = 'false',\n" +
                "  hoodie.datasource.hive_sync.metastore.uris = 'thrift://localhost:9083',\n" +
                "  hoodie.schema.on.read.enable = 'true',\n" +
                "  hoodie.datasource.hive_sync.mode = 'hms'\n" +
                ")\n" +
                "LOCATION 's3a://spark/target_complex'");


    }
}
