package github.jhchee.agg;

import github.jhchee.schema.SourceATable;
import github.jhchee.conf.WriteConf;
import github.jhchee.schema.TargetTable;
import org.apache.hudi.DataSourceReadOptions;
import org.apache.hudi.DataSourceWriteOptions;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;

import java.util.Collections;

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
        Dataset<Row> empty = spark.createDataFrame(Collections.emptyList(), TargetTable.SCHEMA);
        empty.write()
             .format("hudi")
             .option(DataSourceWriteOptions.RECORDKEY_FIELD_OPT_KEY(), TargetTable.PK)
             .option(DataSourceWriteOptions.PRECOMBINE_FIELD_OPT_KEY(), TargetTable.COMBINE_KEY)
             .option(HoodieWriteConfig.TABLE_NAME, TargetTable.TABLE_NAME)
             .option(DataSourceWriteOptions.TABLE_TYPE_OPT_KEY(), "COPY_ON_WRITE")
             .option("hoodie.index.type", "BUCKET")
             .option("hoodie.bucket.index.hash.field", "userId")
             .option("hoodie.bucket.index.num.buckets", "10")
             .option("hoodie.datasource.hive_sync.bucket_sync", "true")
             .options(WriteConf.getHiveSyncOptions("default", TargetTable.TABLE_NAME))
             .mode(SaveMode.Append)
             .save(TargetTable.PATH);

        // snapshot read from a
        spark.read()
             .format("hudi")
             .option("hoodie.table.name", SourceATable.TABLE_NAME)
             .option(DataSourceReadOptions.QUERY_TYPE_OPT_KEY(), DataSourceReadOptions.QUERY_TYPE_SNAPSHOT_OPT_VAL())
             .load(SourceATable.PATH)
             .createOrReplaceTempView(SourceATable.TABLE_NAME);

        spark.sql("" +
                "MERGE INTO target USING source_a as source ON target.userId = source.userId " +
                "WHEN MATCHED THEN UPDATE SET target.persona = struct(source.favoriteEsports), target.updatedAt = source.updatedAt " +
                "WHEN NOT MATCHED THEN INSERT (userId, info, persona, updatedAt) " +
                "VALUES (source.userId, NULL, struct(source.favoriteEsports), source.updatedAt)" +
                "");
        
        Dataset<Row> df = spark.sql("SELECT * FROM target");
        df.show();

        System.out.println("Total count: " + df.count());
    }
}
