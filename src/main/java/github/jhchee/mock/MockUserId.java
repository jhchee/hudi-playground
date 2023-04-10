package github.jhchee.mock;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.SparkSession;

import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class MockUserId {
    public static void main(String[] args) {
        SparkSession spark = SparkSession.builder()
                                         .appName("generate-uuid")
                                         .master("local[*]")
                                         .getOrCreate();

        Dataset<String> userIds = spark.createDataset(IntStream.range(0, 1000000)
                                                               .mapToObj(i -> UUID.randomUUID().toString())
                                                               .collect(Collectors.toList()), Encoders.STRING());
        userIds.withColumnRenamed("value", "userId")
               .repartition(1)
               .write()
               .option("header", "true")
               .csv("./user_ids");


    }
}
