package github.jhchee.mock;

import com.github.javafaker.Faker;
import org.apache.hudi.DataSourceWriteOptions;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.keygen.constant.KeyGeneratorOptions;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.api.java.UDF0;
import org.apache.spark.sql.types.DataTypes;

import static org.apache.spark.sql.functions.*;

public class MockStaticSourceB {
    private static final Faker faker = new Faker();

    public static void main(String[] args) {
        SparkSession spark = SparkSession.builder()
                                         .appName("generate-source-b")
                                         .config("spark.sql.warehouse.dir", "s3a://spark/")
                                         .config("hive.metastore.warehouse.dir", "s3a://spark/")
                                         .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.hudi.catalog.HoodieCatalog")
                                         .config("hive.metastore.uris", "thrift://localhost:9083")
                                         .config("spark.sql.extensions", "org.apache.spark.sql.hudi.HoodieSparkSessionExtension")
                                         .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
                                         .config("spark.hadoop.fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")
                                         .getOrCreate();

        spark.udf().register("fullName", fullName, DataTypes.StringType);
        spark.udf().register("age", age, DataTypes.IntegerType);
        spark.udf().register("streetName", streetName, DataTypes.StringType);
        spark.udf().register("buildingNumber", buildingNumber, DataTypes.StringType);
        spark.udf().register("city", city, DataTypes.StringType);
        spark.udf().register("country", country, DataTypes.StringType);

        Dataset<Row> mockUser = spark.read()
                                     .option("header", "true")
                                     .csv("s3a://spark/user_ids/")
                                     .withColumn("name", call_udf("fullName"))
                                     .withColumn("age", call_udf("age"))
                                     .withColumn("streetName", call_udf("streetName"))
                                     .withColumn("buildingNumber", call_udf("buildingNumber"))
                                     .withColumn("city", call_udf("city"))
                                     .withColumn("country", call_udf("country"))
                                     .withColumn("updatedAt", lit(current_timestamp()));

        mockUser.write()
                .format("hudi")
                .option(KeyGeneratorOptions.RECORDKEY_FIELD_NAME.key(), "userId")
                .option(DataSourceWriteOptions.PRECOMBINE_FIELD_OPT_KEY(), "updatedAt")
                .option(HoodieWriteConfig.TABLE_NAME, "source_b")
                .option(DataSourceWriteOptions.TABLE_TYPE_OPT_KEY(), "COPY_ON_WRITE")
                .option(DataSourceWriteOptions.HIVE_SYNC_ENABLED_OPT_KEY(), "true")
                .option("hoodie.datasource.hive_sync.table", "source_b")
                .option("hoodie.metadata.enable", "false")
                .option(DataSourceWriteOptions.HIVE_USE_JDBC().key(), "false")
                .option(DataSourceWriteOptions.METASTORE_URIS().key(), "thrift://localhost:9083")
                .option(DataSourceWriteOptions.HIVE_SYNC_MODE().key(), "hms")
                .mode(SaveMode.Append)
                .save("s3a://spark/source_b/");
    }

    public static UDF0<String> fullName = () -> faker.name().fullName();
    public static UDF0<String> streetName = () -> faker.address().streetName();
    public static UDF0<String> buildingNumber = () -> faker.address().buildingNumber();
    public static UDF0<String> city = () -> faker.address().city();
    public static UDF0<String> country = () -> faker.address().country();
    public static UDF0<Integer> age = () -> faker.number().numberBetween(18, 80);
}
