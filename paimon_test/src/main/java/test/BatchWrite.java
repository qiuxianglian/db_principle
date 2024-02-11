package test;

import org.apache.paimon.data.BinaryString;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.table.Table;
import org.apache.paimon.table.sink.BatchTableCommit;
import org.apache.paimon.table.sink.BatchTableWrite;
import org.apache.paimon.table.sink.BatchWriteBuilder;
import org.apache.paimon.table.sink.CommitMessage;

import java.util.List;

public class BatchWrite {
    public static void main(String[] args) throws Exception {
        for (int i = 0; i < 1; i++) {
            writeIt();
        }
    }


    static void writeIt() throws Exception {
        // 1. Create a WriteBuilder (Serializable)
        Table table = GetTable.getTable("t1_db", "t1");
        BatchWriteBuilder writeBuilder = table.newBatchWriteBuilder();

        // 2. Write records in distributed tasks
        BatchTableWrite write = writeBuilder.newWrite();

        GenericRow record1 = GenericRow.of(BinaryString.fromString("哈哈"), 14);
        GenericRow record2 = GenericRow.of(BinaryString.fromString("哈哈"), 14);


        write.write(record1);
        write.write(record2);


        List<CommitMessage> messages = write.prepareCommit();

        // 3. Collect all CommitMessages to a global node and commit
        BatchTableCommit commit = writeBuilder.newCommit();
        commit.commit(messages);

        // Abort unsuccessful commit to delete data files
        // commit.abort(messages);
    }
}