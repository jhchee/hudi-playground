package github.jhchee;

import org.apache.spark.sql.SparkSession;

public class CreateTableAgg {
    public static void main(String[] args) {
        SparkSession spark = SparkSession.builder()
                                         .appName("Hudi create table")
                                         .config("spark.sql.warehouse.dir", "s3a://spark/")
                                         .config("hive.metastore.warehouse.dir", "s3a://spark/")
                                         .config("hive.metastore.uris", "thrift://localhost:9083")
                                         .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
                                         .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.hudi.catalog.HoodieCatalog")
                                         .config("spark.sql.extensions", "org.apache.spark.sql.hudi.HoodieSparkSessionExtension")
                                         .config("spark.hadoop.fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")
                                         .config("spark.sql.legacy.parquet.nanosAsLong", "true")
                                         .enableHiveSupport()
                                         .getOrCreate();

        spark.sql("DROP TABLE IF EXISTS source_a");
        spark.sql("DROP TABLE IF EXISTS source_b");
        spark.sql("DROP TABLE IF EXISTS target");

        spark.sql("CREATE TABLE target (\n" +
                "  userId STRING,\n" +
                "  name STRING,\n" +
                "  updatedOn TIMESTAMP\n" +
                ")\n" +
                "USING hudi\n" +
                "OPTIONS (\n" +
                "  type = 'cow',\n" +
                "  primaryKey = 'userId',\n" +
                "  preCombineField = 'updatedOn',\n" +
                "  hoodie.datasource.hive_sync.enable = 'true',\n" +
                "  hoodie.datasource.hive_sync.table = 'target',\n" +
                "  hoodie.metadata.enable = 'false',\n" +
                "  hoodie.datasource.hive_sync.use_jdbc = 'false',\n" +
                "  hoodie.datasource.hive_sync.metastore.uris = 'thrift://localhost:9083',\n" +
                "  hoodie.datasource.hive_sync.mode = 'hms'\n" +
                ")\n" +
                "LOCATION 's3a://spark/target'");
    }
}
