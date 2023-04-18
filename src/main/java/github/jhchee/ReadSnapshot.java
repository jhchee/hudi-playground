package github.jhchee;

import org.apache.hudi.DataSourceReadOptions;
import org.apache.hudi.DataSourceWriteOptions;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class ReadSnapshot {
    public static void main(String[] args) {
        SparkSession spark = SparkSession.builder()
                                         .appName("generate-source-b")
                                         .config("hive.metastore.uris", "thrift://localhost:9083")
                                         .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
                                         .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.hudi.catalog.HoodieCatalog")
                                         .config("spark.sql.extensions", "org.apache.spark.sql.hudi.HoodieSparkSessionExtension")
                                         .config("hive.metastore.warehouse.dir", "s3a://spark/")
                                         .config("spark.hadoop.fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")
                                         .config("spark.sql.legacy.parquet.nanosAsLong", "true")
                                         .enableHiveSupport()
                                         .getOrCreate();
        Dataset<Row> df = spark.sql("SELECT * FROM source_a");
        df.show();
    }
}
