package github.jhchee.schema;

import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

public class TargetTable {
    public static String TABLE_NAME = "target";
    public static StructType SCHEMA = root();

    private static StructType root() {
        StructType schema = new StructType();
        schema = schema.add(new StructField("userId", DataTypes.StringType, false, Metadata.empty()));
        schema = schema.add(new StructField("updatedAt", DataTypes.LongType, false, Metadata.empty()));
        // nested column
        schema = schema.add("persona", persona(), true);
        return schema;
    }

    private static StructType persona() {
        StructType schema = new StructType();
        schema = schema.add(new StructField("favoriteEsports", DataTypes.StringType, true, Metadata.empty()));
//        schema = schema.add(new StructField("favoriteArtist", DataTypes.StringType, true, Metadata.empty()));
        return schema;
    }
}
