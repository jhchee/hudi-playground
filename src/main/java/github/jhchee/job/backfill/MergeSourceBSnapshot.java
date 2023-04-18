package github.jhchee.job.backfill;

import org.apache.hudi.DataSourceReadOptions;
import org.apache.spark.sql.SparkSession;

public class MergeSourceBSnapshot {

    public static void main(String[] args) {
        SparkSession spark = SparkSession.builder()
                                         .appName("Merge Source B to Target [Snapshot]")
                                         .config("hive.metastore.uris", "thrift://localhost:9083")
                                         .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
                                         .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.hudi.catalog.HoodieCatalog")
                                         .config("spark.sql.extensions", "org.apache.spark.sql.hudi.HoodieSparkSessionExtension")
                                         .config("spark.hadoop.fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")
                                         .config("spark.sql.legacy.parquet.nanosAsLong", "true")
                                         .config("spark.sql.warehouse.dir", "s3a://spark/")
                                         .enableHiveSupport()
                                         .getOrCreate();

        // read snapshot
        spark.read()
             .format("hudi")
             .option("hoodie.table.name", "source_b")
             .option(DataSourceReadOptions.QUERY_TYPE_OPT_KEY(), DataSourceReadOptions.QUERY_TYPE_SNAPSHOT_OPT_VAL())
             .load("s3a://spark/source_b/")
             .createOrReplaceTempView("source");


        spark.sql("" +
                "MERGE INTO target USING source ON target.userId = source.userId " +
                "WHEN MATCHED THEN UPDATE SET target.name = source.name, target.updatedOn = source.updatedAt " +
                "WHEN NOT MATCHED THEN INSERT (userId, name, updatedOn) VALUES (source.userId, source.name, source.updatedAt)" +
                "");
    }
}
