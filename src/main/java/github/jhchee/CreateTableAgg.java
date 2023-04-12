package github.jhchee;

import org.apache.spark.sql.SparkSession;

public class CreateTableAgg {
    public static void main(String[] args) {
        SparkSession spark = SparkSession.builder()
                                         .appName("Hudi create table")
                                         .master("local[1]")
                                         .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
                                         .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.hudi.catalog.HoodieCatalog")
                                         .config("spark.sql.extensions", "org.apache.spark.sql.hudi.HoodieSparkSessionExtension")
                                         .getOrCreate();

//        spark.sql("CREATE TABLE target_tb (\n" +
//                "  userId STRING,\n" +
//                "  name STRING\n" +
//                ")\n" +
//                "USING org.apache.hudi\n" +
//                "OPTIONS (\n" +
//                "  'hoodie.datasource.write.recordkey.field' 'userId',\n" +
//                "  'hoodie.datasource.write.precombine.field' 'updatedOn'\n" +
//                ")\n" +
//                "LOCATION '/tmp/hudi/agg/target'");

        spark.sql("create table hudi_existing_tbl using org.apache.hudi\n" +
                "location '/tmp/hudi/hudi_existing_table';");
    }
}
