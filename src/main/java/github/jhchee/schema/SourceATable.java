package github.jhchee.schema;

import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

public class SourceATable {
    public static StructType SCHEMA = root();
    public static String TABLE_NAME = "source_a";
    public static String PATH = "s3a://spark/source_a/";
    public static String PK = "userId";
    public static String COMBINE_KEY = "updatedAt";

    private static StructType root() {
        StructType schema = new StructType();
        schema = schema.add(new StructField("userId", DataTypes.StringType, false, Metadata.empty()));
        schema = schema.add(new StructField("updatedAt", DataTypes.TimestampType, false, Metadata.empty()));
        schema = schema.add("favoriteEsports", DataTypes.StringType, true);
        schema = schema.add("favoriteArtist", DataTypes.StringType, true);
        schema = schema.add("favoriteColor", DataTypes.StringType, true);
        schema = schema.add("favoriteHarryPotterCharacter", DataTypes.StringType, true);
        return schema;
    }
}
