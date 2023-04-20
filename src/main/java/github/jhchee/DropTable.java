package github.jhchee;

import org.apache.spark.sql.SparkSession;

public class DropTable {
    public static void main(String[] args) {
        SparkSession spark = SparkSession.builder()
                                         .appName("Drop Hudi Tables.")
                                         .config("spark.sql.warehouse.dir", "s3a://spark/")
                                         .config("hive.metastore.uris", "thrift://localhost:9083")
                                         .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
                                         .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.hudi.catalog.HoodieCatalog")
                                         .config("spark.sql.extensions", "org.apache.spark.sql.hudi.HoodieSparkSessionExtension")
                                         .config("spark.sql.legacy.parquet.nanosAsLong", "true")
                                         .config("hoodie.schema.on.read.enable", "true")
                                         .enableHiveSupport()
                                         .getOrCreate();

        spark.sql("DROP TABLE IF EXISTS default.default");
        spark.sql("DROP TABLE IF EXISTS source_a");
        spark.sql("DROP TABLE IF EXISTS source_b");
        spark.sql("DROP TABLE IF EXISTS target");
        spark.sql("DROP TABLE IF EXISTS target_complex");
    }
}
