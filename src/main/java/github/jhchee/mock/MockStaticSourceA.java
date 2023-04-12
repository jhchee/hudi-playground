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

public class MockStaticSourceA {
    private static final Faker faker = new Faker();
    private static final String TABLE_NAME = "source_a";
    private static final String TABLE_PATH = "/tmp/hudi/raw/" + TABLE_NAME;

    public static void main(String[] args) {
        SparkSession spark = SparkSession.builder()
                                         .appName("generate-source-a")
                                         .master("local[1]")
                                         .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
                                         .getOrCreate();

        spark.udf().register("favoriteEsports", favoriteEsports, DataTypes.StringType);
        spark.udf().register("favoriteArtist", favoriteArtist, DataTypes.StringType);
        spark.udf().register("favoriteColor", favoriteColor, DataTypes.StringType);
        spark.udf().register("favoriteHarryPotterCharacter", favoriteHarryPotterCharacter, DataTypes.StringType);


        Dataset<Row> mockUser = spark.read()
                                     .option("header", "true")
                                     .csv("./user_ids/")
                                     .withColumn("favoriteEsports", call_udf("favoriteEsports"))
                                     .withColumn("favoriteArtist", call_udf("favoriteArtist"))
                                     .withColumn("favoriteColor", call_udf("favoriteColor"))
                                     .withColumn("favoriteHarryPotterCharacter", call_udf("favoriteHarryPotterCharacter"))
                                     .withColumn("updatedAt", lit(current_timestamp()));

        mockUser.write()
                .format("org.apache.hudi")
                .option(KeyGeneratorOptions.RECORDKEY_FIELD_NAME.key(), "userId")
//                .option(KeyGeneratorOptions.PARTITIONPATH_FIELD_NAME.key(), "updatedAt")
                .option(HoodieWriteConfig.PRECOMBINE_FIELD_NAME.key(), "updatedAt")
                .option(HoodieWriteConfig.TBL_NAME.key(), TABLE_NAME)
                .mode(SaveMode.Append)
                .save(TABLE_PATH);
    }

    // faker
    public static UDF0<String> favoriteEsports = () -> faker.esports().game();
    public static UDF0<String> favoriteArtist = () -> faker.artist().name();
    public static UDF0<String> favoriteColor = () -> faker.color().name();
    public static UDF0<String> favoriteHarryPotterCharacter = () -> faker.harryPotter().character();
}
