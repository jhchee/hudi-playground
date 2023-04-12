package github.jhchee;

import org.apache.hudi.DataSourceReadOptions;
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
                .format("org.apache.hudi")
                .option("hoodie.table.name", "source_a")
                .option(DataSourceReadOptions.QUERY_TYPE_OPT_KEY(), DataSourceReadOptions.QUERY_TYPE_SNAPSHOT_OPT_VAL())
                .load("/tmp/hudi/raw/source_a");

        hudiIncQueryDF.show(10);
        System.out.println(hudiIncQueryDF.count());
    }
}
