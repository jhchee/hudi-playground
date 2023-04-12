package github.jhchee.mock;

import com.github.javafaker.Faker;
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
    private static final String TABLE_NAME = "source_b";
    private static final String TABLE_PATH = "/tmp/hudi/raw/" + TABLE_NAME;

    public static void main(String[] args) {
        SparkSession spark = SparkSession.builder()
                                         .appName("generate-source-b")
                                         .master("local[1]")
                                         .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
                                         .getOrCreate();

        spark.udf().register("fullName", fullName, DataTypes.StringType);
        spark.udf().register("age", age, DataTypes.IntegerType);
        spark.udf().register("streetName", streetName, DataTypes.StringType);
        spark.udf().register("buildingNumber", buildingNumber, DataTypes.StringType);
        spark.udf().register("city", city, DataTypes.StringType);
        spark.udf().register("country", country, DataTypes.StringType);

        Dataset<Row> mockUser = spark.read()
                                     .option("header", "true")
                                     .csv("./user_ids/")
                                     .withColumn("name", call_udf("fullName"))
                                     .withColumn("age", call_udf("age"))
                                     .withColumn("streetName", call_udf("streetName"))
                                     .withColumn("buildingNumber", call_udf("buildingNumber"))
                                     .withColumn("city", call_udf("city"))
                                     .withColumn("country", call_udf("country"))
                                     .withColumn("updatedAt", lit(current_timestamp()));

        mockUser.write()
                .format("org.apache.hudi")
                .option(KeyGeneratorOptions.RECORDKEY_FIELD_NAME.key(), "userId")
                .option(HoodieWriteConfig.PRECOMBINE_FIELD_NAME.key(), "updatedAt")
                .option(HoodieWriteConfig.TBL_NAME.key(), TABLE_NAME)
                .mode(SaveMode.Append)
                .save(TABLE_PATH);
    }

    public static UDF0<String> fullName = () -> faker.name().fullName();
    public static UDF0<String> streetName = () -> faker.address().streetName();
    public static UDF0<String> buildingNumber = () -> faker.address().buildingNumber();
    public static UDF0<String> city = () -> faker.address().city();
    public static UDF0<String> country = () -> faker.address().country();
    public static UDF0<Integer> age = () -> faker.number().numberBetween(18, 80);
}
