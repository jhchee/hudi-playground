package github.jhchee.schema;

import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

public class SourceBTable {
    public static StructType SCHEMA = root();
    public static String TABLE_NAME = "source_b";
    public static String PATH = "s3a://spark/source_b/";
    public static String PK = "userId";
    public static String COMBINE_KEY = "updatedAt";

    private static StructType root() {
        StructType schema = new StructType();
        schema = schema.add(new StructField("userId", DataTypes.StringType, false, Metadata.empty()));
        schema = schema.add(new StructField("updatedAt", DataTypes.TimestampType, false, Metadata.empty()));
        schema = schema.add("name", DataTypes.StringType, true);
        schema = schema.add("age", DataTypes.IntegerType, true);
        schema = schema.add("streetName", DataTypes.StringType, true);
        schema = schema.add("buildingNumber", DataTypes.StringType, true);
        schema = schema.add("city", DataTypes.StringType, true);
        schema = schema.add("country", DataTypes.StringType, true);
        return schema;
    }
}
