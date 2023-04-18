package github.jhchee.agg;

import org.apache.hudi.DataSourceReadOptions;
import org.apache.spark.sql.SparkSession;

public class MergeSnapshot {

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
        spark.read()
             .format("hudi")
             .option("hoodie.table.name", "source_a")
             .option(DataSourceReadOptions.QUERY_TYPE_OPT_KEY(), DataSourceReadOptions.QUERY_TYPE_SNAPSHOT_OPT_VAL())
             .load("s3a://spark/source_a/")
             .createOrReplaceTempView("source_a");
        spark.sql("" +
                "MERGE INTO target USING source_a as source ON target.userId = source.userId " +
                "WHEN MATCHED THEN UPDATE SET target.favoriteEsports = source.favoriteEsports, target.updatedAt = source.updatedAt " +
                "WHEN NOT MATCHED THEN INSERT (userId, name, favoriteEsports, updatedAt) " +
                "VALUES (source.userId, NULL, source.favoriteEsports, source.updatedAt)" +
                "");

        // snapshot read from b
        spark.read()
             .format("hudi")
             .option("hoodie.table.name", "source_b")
             .option(DataSourceReadOptions.QUERY_TYPE_OPT_KEY(), DataSourceReadOptions.QUERY_TYPE_SNAPSHOT_OPT_VAL())
             .load("s3a://spark/source_b/")
             .createOrReplaceTempView("source_b");
        spark.sql("" +
                "MERGE INTO target USING source_b as source ON target.userId = source.userId " +
                "WHEN MATCHED THEN UPDATE SET target.name = source.name, target.updatedAt = source.updatedAt " +
                "WHEN NOT MATCHED THEN INSERT (userId, name, favoriteEsports, updatedAt) " +
                "VALUES (source.userId, source.name, NULL, source.updatedAt)" +
                "");
    }
}
