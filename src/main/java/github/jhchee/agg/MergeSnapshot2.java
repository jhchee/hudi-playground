package github.jhchee.agg;

import org.apache.hudi.DataSourceReadOptions;
import org.apache.hudi.DataSourceWriteOptions;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;

public class MergeSnapshot2 {

    public static void main(String[] args) {
        SparkSession spark = SparkSession.builder()
                                         .appName("Merge snapshot read")
                                         .config("hive.metastore.uris", "thrift://localhost:9083")
                                         .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
                                         .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.hudi.catalog.HoodieCatalog")
                                         .config("spark.sql.extensions", "org.apache.spark.sql.hudi.HoodieSparkSessionExtension")
                                         .config("spark.sql.legacy.parquet.nanosAsLong", "true")
                                         .config("spark.sql.warehouse.dir", "s3a://spark/")
                                         .enableHiveSupport()
                                         .getOrCreate();

        // snapshot read from a
        Dataset<Row> source = spark.read()
                                   .format("hudi")
                                   .option("hoodie.table.name", "source_a")
                                   .option(DataSourceReadOptions.QUERY_TYPE_OPT_KEY(), DataSourceReadOptions.QUERY_TYPE_SNAPSHOT_OPT_VAL())
                                   .load("s3a://spark/source_a/");

        Dataset<Row> target = spark.read()
                                   .format("hudi")
                                   .option("hoodie.table.name", "target_complex")
                                   .option(DataSourceReadOptions.QUERY_TYPE_OPT_KEY(), DataSourceReadOptions.QUERY_TYPE_SNAPSHOT_OPT_VAL())
                                   .load("s3a://spark/target_complex/");

        target = target.as("target").join(source.as("source"), target.col("userId").equalTo(source.col("userId")), "outer")
                       .selectExpr("source.userId as userId",
                               "source.updatedAt as updatedAt",
                               "struct(source.*) as nested");

        target.write()
              .format("hudi")
              .option(DataSourceWriteOptions.RECORDKEY_FIELD_OPT_KEY(), "userId")
              .option(DataSourceWriteOptions.PRECOMBINE_FIELD_OPT_KEY(), "updatedAt")
              .option(HoodieWriteConfig.TABLE_NAME, "target_complex")
              .option(DataSourceWriteOptions.TABLE_TYPE_OPT_KEY(), "COPY_ON_WRITE")
              // hive sync option
              .option(DataSourceWriteOptions.HIVE_SYNC_ENABLED_OPT_KEY(), "true")
              .option("hoodie.datasource.hive_sync.table", "target_complex")
              .option("hoodie.metadata.enable", "false") // minio docker issue
              .option(DataSourceWriteOptions.HIVE_USE_JDBC().key(), "false")
              .option(DataSourceWriteOptions.METASTORE_URIS().key(), "thrift://localhost:9083")
              .option(DataSourceWriteOptions.HIVE_SYNC_MODE().key(), "hms")
              .mode(SaveMode.Append)
              .save("s3a://spark/target_complex/");
    }
}
