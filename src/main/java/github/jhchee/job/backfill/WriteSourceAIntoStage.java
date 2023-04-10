package github.jhchee.job.backfill;

import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.keygen.constant.KeyGeneratorOptions;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;

import static org.apache.spark.sql.functions.*;

public class WriteSourceAIntoStage {
    public static void main(String[] args) {
        SparkSession spark = SparkSession.builder()
                                         .appName("write-source-a")
                                         .master("local[1]")
                                         .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
                                         .getOrCreate();


        Dataset<Row> mockUser = spark.read()
                                     .option("header", "true")
                                     .csv("./user_ids/")

                                     .withColumn("favoriteHarryPotterCharacter", call_udf("favoriteHarryPotterCharacter"))
                                     .withColumn("updatedAt", lit(current_timestamp()));

//        mockUser.write()
//                .format("org.apache.hudi")
//                .option(KeyGeneratorOptions.RECORDKEY_FIELD_NAME.key(), "userId")
//                .option(KeyGeneratorOptions.PARTITIONPATH_FIELD_NAME.key(), "updatedAt")
//                .option(HoodieWriteConfig.PRECOMBINE_FIELD_NAME.key(), "updatedAt")
//                .option(HoodieWriteConfig.TBL_NAME.key(), TABLE_NAME)
//                .mode(SaveMode.Append)
//                .save(TABLE_PATH);
    }
}
