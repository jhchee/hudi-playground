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
                                         .master("local[1]")
                                         .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
                                         .getOrCreate();
        Dataset<Row> hudiIncQueryDF = spark
                .read()
                .format("hudi")
                .option(DataSourceWriteOptions.RECORDKEY_FIELD_OPT_KEY(), "userId")
                .option(DataSourceWriteOptions.PRECOMBINE_FIELD_OPT_KEY(), "updatedAt")
                .option(HoodieWriteConfig.TABLE_NAME, "source_a")
                .option(DataSourceReadOptions.QUERY_TYPE_OPT_KEY(), DataSourceReadOptions.QUERY_TYPE_SNAPSHOT_OPT_VAL())
                .load("/tmp/hudi/raw/source_a");

        hudiIncQueryDF.show(10);
        System.out.println(hudiIncQueryDF.count());
    }
}
