package dynamok.sink;

import com.amazonaws.services.dynamodbv2.model.WriteRequest;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.Assert;
import org.junit.Test;

import java.util.*;

public class DynamoDbSinkTaskTest {

    @Test
    public void toWritesByTable() {
        final String tableName = "test";
        final int recordCount = 10;

        Map<String, String> config = new HashMap<>();
        config.put("primary.key.attribute", "col1");
        config.put("region", "ap-northeast-1");
        config.put("ignore.record.key", "true");
        config.put("batch.size", String.valueOf(recordCount));
        DynamoDbSinkTask task = new DynamoDbSinkTask();
        task.start(config);

        // all same
        Collection<SinkRecord> records = new ArrayList<>();
        for (int i = 0; i < recordCount; i ++ ) {
            Map<String, Object> data = new HashMap<>();
            data.put("col1", "A");
            data.put("col2", "B");
            records.add(new SinkRecord(tableName, 0, null, null, null, data, 0));
        }
        Map<String, List<WriteRequest>> actual = task.toWritesByTable(records.iterator());
        List<WriteRequest> actualRecord = actual.get(tableName);
        Assert.assertEquals(actualRecord.size(), 1);

        // all different
        records.clear();
        for (int i = 0; i < recordCount; i ++ ) {
            Map<String, Object> data = new HashMap<>();
            data.put("col1", i);
            data.put("col2", "B");
            records.add(new SinkRecord(tableName, 0, null, null, null, data, 0));
        }
        actual = task.toWritesByTable(records.iterator());
        actualRecord = actual.get(tableName);
        Assert.assertEquals(actualRecord.size(), 10);

    }
}
