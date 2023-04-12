package github.jhchee.job.backfill;

import org.apache.hudi.DataSourceReadOptions;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class MergeSourceBSnapshot {

    public static void main(String[] args) {
        SparkSession spark = SparkSession.builder()
                                         .appName("Merge Source B to Target [Snapshot]")
                                         .master("local[1]")
                                         .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
                                         .getOrCreate();

        Dataset<Row> source = spark.read()
                                   .format("org.apache.hudi")
                                   .option("hoodie.table.name", "source_b")
                                   .option(DataSourceReadOptions.QUERY_TYPE_OPT_KEY(), DataSourceReadOptions.QUERY_TYPE_SNAPSHOT_OPT_VAL())
                                   .load("/tmp/hudi/raw/source_b");

        Dataset<Row> target = spark.read()
                                   .format("org.apache.hudi")
                                   .option("hoodie.table.name", "target")
                                   .option(DataSourceReadOptions.QUERY_TYPE_OPT_KEY(), DataSourceReadOptions.QUERY_TYPE_SNAPSHOT_OPT_VAL())
                                   .load("/tmp/hudi/agg/target");

        source.createOrReplaceTempView("source");
        target.createOrReplaceTempView("target");

        spark.sql("" +
                "MERGE INTO target USING source ON target.userId = source.userId " +
                "WHEN MATCHED THEN UPDATE SET target.name = source.name " +
                "WHEN NOT MATCHED THEN INSERT (userId, name) VALUES (source.userId, source.name)" +
                "");
    }
}
