package github.jhchee.schema;

import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

public class TargetSchema {
    public static StructType SCHEMA = new StructType();

    static {
        SCHEMA.add(new StructField("userId", DataTypes.StringType, false, Metadata.empty()));
        SCHEMA.add(new StructField("updatedAt", DataTypes.TimestampType, false, Metadata.empty()));
        StructType nested = new StructType()
                .add(new StructField("favoriteEsports", DataTypes.TimestampType, true, Metadata.empty()));
        SCHEMA.add("nested", nested, true);
    }
}
