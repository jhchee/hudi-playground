package github.jhchee;

import com.google.common.collect.Lists;
import org.apache.avro.Schema;
import org.apache.avro.generic.IndexedRecord;
import org.apache.hudi.client.HoodieJavaWriteClient;
import org.apache.hudi.client.WriteStatus;
import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieRecordPayload;
import org.apache.hudi.common.util.Option;

import java.io.IOException;
import java.time.Instant;
import java.util.List;
import java.util.Objects;

public class JavaClient {
    public static void main(String[] args) {
        TypedProperties props = new TypedProperties();
        props.setProperty("hoodie.datasource.write.recordkey.field", "id");
        props.setProperty("hoodie.datasource.write.partitionpath.field", "date");
        props.setProperty("hoodie.datasource.write.table.name", tableName);
        props.setProperty("hoodie.datasource.write.operation", "upsert");
        HoodieJavaWriteClient writeClient = new HoodieJavaWriteClient<>(new HoodieJavaWriteConfigBuilder()
                .withPath(basePath)
                .withProps(props)
                .build());
        HoodieRecordPayload payload1 = new MyRecordPayload("id1", "2023-04-09", "Some data");
        HoodieRecordPayload payload2 = new MyRecordPayload("id2", "2023-04-10", "Some more data");
        HoodieRecord record1 = new HoodieRecord<>("id1", payload1);
        HoodieRecord record2 = new HoodieRecord<>("id2", payload2);
        List<WriteStatus> statuses = writeClient.upsert(Lists.asList(record1, record2), Instant.now());

        // print the status of each write operation
        for (WriteStatus status : statuses) {
            System.out.println(status);
        }
    }

    public static class MyRecordPayload implements HoodieRecordPayload {
        private final String id;
        private final String date;
        private final String data;

        public MyRecordPayload(String id, String date, String data) {
            this.id = id;
            this.date = date;
            this.data = data;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            MyRecordPayload that = (MyRecordPayload) o;
            return Objects.equals(id, that.id) &&
                    Objects.equals(date, that.date) &&
                    Objects.equals(data, that.data);
        }

        @Override
        public HoodieRecordPayload preCombine(HoodieRecordPayload hoodieRecordPayload) {
            return null;
        }

        @Override
        public Option<IndexedRecord> combineAndGetUpdateValue(IndexedRecord indexedRecord, Schema schema) throws IOException {
            return null;
        }

        @Override
        public Option<IndexedRecord> getInsertValue(Schema schema) throws IOException {
            return null;
        }
    }
}

