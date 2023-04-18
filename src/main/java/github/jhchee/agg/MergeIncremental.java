package github.jhchee.agg;

import org.apache.hudi.DataSourceReadOptions;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.DataStreamWriter;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.StreamingQueryException;
import org.apache.spark.sql.streaming.Trigger;

import java.util.concurrent.TimeoutException;

public class MergeIncremental {
    public static void main(String[] args) throws TimeoutException, StreamingQueryException {
        SparkSession spark = SparkSession.builder()
                                         .appName("Merge incrementally")
                                         .config("hive.metastore.uris", "thrift://localhost:9083")
                                         .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
                                         .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.hudi.catalog.HoodieCatalog")
                                         .config("spark.sql.extensions", "org.apache.spark.sql.hudi.HoodieSparkSessionExtension")
                                         .config("spark.sql.legacy.parquet.nanosAsLong", "true")
                                         .config("spark.sql.warehouse.dir", "s3a://spark/")
                                         .enableHiveSupport()
                                         .getOrCreate();

        Dataset<Row> streamingDataset = spark.readStream()
                                             .format("hudi")
                                             .option("hoodie.table.name", "source_b")
                                             .option(DataSourceReadOptions.QUERY_TYPE_OPT_KEY(), DataSourceReadOptions.QUERY_TYPE_SNAPSHOT_OPT_VAL())
                                             .load("/tmp/hudi/raw/source_b");

        DataStreamWriter<Row> dataStreamWriter = streamingDataset.writeStream()
                                                                 .trigger(Trigger.Once())
                                                                 .format("console")
                                                                 .option("checkpointLocation", "/tmp/checkpointDir")
                                                                 .outputMode("append");
        StreamingQuery query = dataStreamWriter.start();
        query.awaitTermination();
    }
}
