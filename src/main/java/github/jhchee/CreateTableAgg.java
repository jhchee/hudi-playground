package github.jhchee;

import org.apache.spark.sql.SparkSession;

public class CreateTableAgg {
    public static void main(String[] args) {
        SparkSession spark = SparkSession.builder()
                                         .appName("Hudi create table")
                                         .master("local[1]")
//                                         .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
//                                         .config("spark.sql.extensions", "org.apache.spark.sql.hudi.HoodieSparkSessionExtension")
                                         .getOrCreate();

        spark.sql("CREATE TABLE target (\n" +
                "  userId STRING,\n" +
                "  name STRING,\n" +
                "  updatedOn TIMESTAMP\n" +
                ")\n" +
                "USING hudi\n" +
                "OPTIONS (\n" +
                "  'hoodie.datasource.write.recordkey.field' 'userId',\n" +
                "  'hoodie.datasource.write.precombine.field' 'updatedOn'\n" +
                ")\n" +
                "LOCATION '/tmp/hudi/agg/target'");
    }
}
