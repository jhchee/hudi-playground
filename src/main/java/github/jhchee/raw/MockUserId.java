package github.jhchee.raw;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.SparkSession;

import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class MockUserId {
    public static void main(String[] args) {
        SparkSession spark = SparkSession.builder()
                                         .appName("generate uuid")
                                         .config("hive.metastore.uris", "thrift://localhost:9083")
                                         .config("spark.hadoop.fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")
                                         .enableHiveSupport()
                                         .getOrCreate();

        Dataset<String> userIds = spark.createDataset(IntStream.range(0, 1_000)
                                                               .mapToObj(i -> UUID.randomUUID().toString())
                                                               .collect(Collectors.toList()), Encoders.STRING());
        userIds.withColumnRenamed("value", "userId")
               .write()
               .option("header", "true")
               .csv("s3a://spark/user_ids/");
    }
}
