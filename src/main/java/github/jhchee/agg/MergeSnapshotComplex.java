package github.jhchee.agg;

import github.jhchee.util.WriteUtils;
import github.jhchee.schema.TargetSchema;
import org.apache.hudi.DataSourceReadOptions;
import org.apache.hudi.DataSourceWriteOptions;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;

import java.util.ArrayList;
import java.util.Collections;

public class MergeSnapshotComplex {

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
        Dataset<Row> empty = spark.createDataFrame(Collections.emptyList(), TargetSchema.SCHEMA);
        empty.write()
             .format("hudi")
             .option(DataSourceWriteOptions.RECORDKEY_FIELD_OPT_KEY(), "userId")
             .option(DataSourceWriteOptions.PRECOMBINE_FIELD_OPT_KEY(), "updatedAt")
             .option(HoodieWriteConfig.TABLE_NAME, "target_complex")
             .option(DataSourceWriteOptions.TABLE_TYPE_OPT_KEY(), "COPY_ON_WRITE")
             .options(WriteUtils.getHiveSyncOptions("default", "target_complex"))
             .mode(SaveMode.Append)
             .save("s3a://spark/target_complex/");
        empty.printSchema();
//        // snapshot read from a
//        spark.read()
//             .format("hudi")
//             .option("hoodie.table.name", "source_a")
//             .option(DataSourceReadOptions.QUERY_TYPE_OPT_KEY(), DataSourceReadOptions.QUERY_TYPE_SNAPSHOT_OPT_VAL())
//             .load("s3a://spark/source_a/")
//             .createOrReplaceTempView("source_a");
//
//        spark.sql("" +
//                "MERGE INTO target_complex as target USING source_a as source ON target.userId = source.userId " +
//                "WHEN MATCHED THEN UPDATE SET target.nested = struct(source.favoriteEsports), target.updatedAt = source.updatedAt " +
//                "WHEN NOT MATCHED THEN INSERT (userId, nested, updatedAt) " +
//                "VALUES (source.userId, struct(source.favoriteEsports), source.updatedAt)" +
//                "");
    }
}