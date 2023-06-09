package github.jhchee.raw;

import com.github.javafaker.Faker;
import github.jhchee.schema.SourceATable;
import github.jhchee.conf.WriteConf;
import org.apache.hudi.DataSourceWriteOptions;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.api.java.UDF0;
import org.apache.spark.sql.types.DataTypes;

import static org.apache.spark.sql.functions.*;

public class MockSourceA {
    private static final Faker faker = new Faker();

    public static void main(String[] args) {
        SparkSession spark = SparkSession.builder()
                                         .appName("Mock data for source A.")
                                         .config("spark.sql.warehouse.dir", "s3a://spark/")
                                         .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.hudi.catalog.HoodieCatalog")
                                         .config("hive.metastore.uris", "thrift://localhost:9083")
                                         .config("spark.sql.extensions", "org.apache.spark.sql.hudi.HoodieSparkSessionExtension")
                                         .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
                                         .getOrCreate();

        spark.udf().register("favoriteEsports", favoriteEsports, DataTypes.StringType);
        spark.udf().register("favoriteArtist", favoriteArtist, DataTypes.StringType);
        spark.udf().register("favoriteColor", favoriteColor, DataTypes.StringType);
        spark.udf().register("favoriteHarryPotterCharacter", favoriteHarryPotterCharacter, DataTypes.StringType);

        Dataset<Row> mockUser = spark.read()
                                     .option("header", "true")
                                     .csv("s3a://spark/user_ids/")
                                     .withColumn("favoriteEsports", call_udf("favoriteEsports"))
                                     .withColumn("favoriteArtist", call_udf("favoriteArtist"))
                                     .withColumn("favoriteColor", call_udf("favoriteColor"))
                                     .withColumn("favoriteHarryPotterCharacter", call_udf("favoriteHarryPotterCharacter"))
                                     .withColumn("updatedAt", lit(current_timestamp()));

        mockUser.write()
                .format("hudi")
                .option(DataSourceWriteOptions.RECORDKEY_FIELD_OPT_KEY(), SourceATable.PK)
                .option(DataSourceWriteOptions.PRECOMBINE_FIELD_OPT_KEY(), SourceATable.COMBINE_KEY)
                .option(HoodieWriteConfig.TABLE_NAME, SourceATable.TABLE_NAME)
                .option(DataSourceWriteOptions.TABLE_TYPE_OPT_KEY(), "COPY_ON_WRITE")
                .options(WriteConf.getHiveSyncOptions("default", SourceATable.TABLE_NAME))
                .mode(SaveMode.Append)
                .save(SourceATable.PATH);
    }

    public static UDF0<String> favoriteEsports = () -> faker.esports().game();
    public static UDF0<String> favoriteArtist = () -> faker.artist().name();
    public static UDF0<String> favoriteColor = () -> faker.color().name();
    public static UDF0<String> favoriteHarryPotterCharacter = () -> faker.harryPotter().character();
}
