package github.jhchee.schema;

import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

public class TargetTable {
    public static StructType SCHEMA = root();

    public static StructType root() {
        StructType schema = new StructType();
        schema = schema.add(new StructField("userId", DataTypes.StringType, false, Metadata.empty()));
        schema = schema.add(new StructField("updatedAt", DataTypes.TimestampType, false, Metadata.empty()));
        // nested column
        schema = schema.add("nested", nested(), true);
        return schema;
    }

    public static StructType nested() {
        StructType schema = new StructType();
        schema = schema.add(new StructField("favoriteEsports", DataTypes.StringType, true, Metadata.empty()));
        return schema;
    }
}
