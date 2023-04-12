package github.jhchee.job.incremental;

import org.apache.hudi.DataSourceReadOptions;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.DataStreamWriter;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.StreamingQueryException;
import org.apache.spark.sql.streaming.Trigger;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

public class MergeSourceBIncremental {
    public static void main(String[] args) throws TimeoutException, StreamingQueryException {
        SparkSession spark = SparkSession.builder()
                                         .appName("Merge Source B to Target [Streaming]")
                                         .master("local[1]")
                                         .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
                                         .getOrCreate();

        Dataset<Row> streamingDataset = spark.readStream()
                                             .format("org.apache.hudi")
                                             .option("hoodie.table.name", "source_b")
                                             .option(DataSourceReadOptions.QUERY_TYPE_OPT_KEY(), DataSourceReadOptions.QUERY_TYPE_SNAPSHOT_OPT_VAL())
                                             .load("/tmp/hudi/raw/source_b");

        DataStreamWriter<Row> dataStreamWriter = streamingDataset.writeStream()
                                                                 .trigger(Trigger.ProcessingTime(1, TimeUnit.SECONDS))
                                                                 .format("console")
                                                                 .option("checkpointLocation", "/tmp/checkpointDir")
                                                                 .outputMode("append");
        StreamingQuery query = dataStreamWriter.start();
        query.awaitTermination();
    }
}
