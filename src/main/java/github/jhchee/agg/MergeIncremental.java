package github.jhchee.agg;

import github.jhchee.schema.SourceATable;
import github.jhchee.schema.TargetTable;
import github.jhchee.conf.WriteConf;
import org.apache.hudi.DataSourceReadOptions;
import org.apache.hudi.DataSourceWriteOptions;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.spark.api.java.function.VoidFunction2;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.DataStreamWriter;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.StreamingQueryException;
import org.apache.spark.sql.streaming.Trigger;

import java.util.Collections;
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

        Dataset<Row> empty = spark.createDataFrame(Collections.emptyList(), TargetTable.SCHEMA);
        empty.write()
             .format("hudi")
             .option(DataSourceWriteOptions.RECORDKEY_FIELD_OPT_KEY(), TargetTable.PK)
             .option(DataSourceWriteOptions.PRECOMBINE_FIELD_OPT_KEY(), TargetTable.COMBINE_KEY)
             .option(HoodieWriteConfig.TABLE_NAME, TargetTable.TABLE_NAME)
             .option(DataSourceWriteOptions.TABLE_TYPE_OPT_KEY(), "COPY_ON_WRITE")
             .options(WriteConf.getHiveSyncOptions("default", TargetTable.TABLE_NAME))
             .mode(SaveMode.Append)
             .save(TargetTable.PATH);

        String query = "MERGE INTO target as target USING source ON target.userId = source.userId " +
                "WHEN MATCHED THEN UPDATE SET target.persona = struct(source.favoriteEsports), target.updatedAt = source.updatedAt " +
                "WHEN NOT MATCHED THEN INSERT (userId, info, persona, updatedAt) " +
                "VALUES (source.userId, NULL, struct(source.favoriteEsports), source.updatedAt)";
        incrementalMerge(spark, SourceATable.PATH, SourceATable.TABLE_NAME, TargetTable.TABLE_NAME, query);
    }

    public static void incrementalMerge(SparkSession spark, String sourcePath, String sourceTable, String targetTable, String mergeQuery) throws TimeoutException, StreamingQueryException {
        Dataset<Row> source = spark.readStream()
                                   .format("hudi")
                                   .option("hoodie.table.name", sourceTable)
                                   .option(DataSourceReadOptions.QUERY_TYPE_OPT_KEY(), DataSourceReadOptions.QUERY_TYPE_INCREMENTAL_OPT_VAL())
                                   .load(sourcePath);
        String checkpointLocation = String.format("s3a://spark/checkpoint/merge%sInto%s", sourceTable, targetTable);
        DataStreamWriter<Row> dataStreamWriter = source.writeStream()
                                                       .format("hudi")
                                                       .trigger(Trigger.AvailableNow())
                                                       .foreachBatch((VoidFunction2<Dataset<Row>, Long>) (sourceDf, batchId) -> executeMergeQuery(sourceDf, batchId, mergeQuery))
                                                       .option("checkpointLocation", checkpointLocation);
        StreamingQuery query = dataStreamWriter.start();
        query.awaitTermination();
    }

    public static void executeMergeQuery(Dataset<Row> sourceDf, Long batchId, String mergeQuery) {
        System.out.println("Batch id: " + batchId);
        sourceDf.createOrReplaceTempView("source");
        sourceDf.sparkSession().sql(mergeQuery);
    }
}
