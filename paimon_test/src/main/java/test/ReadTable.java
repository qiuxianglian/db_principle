package test;

import org.apache.paimon.data.InternalRow;
import org.apache.paimon.predicate.Predicate;
import org.apache.paimon.predicate.PredicateBuilder;
import org.apache.paimon.reader.RecordReader;
import org.apache.paimon.table.Table;
import org.apache.paimon.table.source.ReadBuilder;
import org.apache.paimon.table.source.Split;
import org.apache.paimon.table.source.TableRead;
import org.apache.paimon.table.source.TableScan;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.RowType;

import com.google.common.collect.Lists;

import java.util.List;

public class ReadTable {

    public static void main(String[] args) throws Exception {
        // 1. Create a ReadBuilder and push filter (`withFilter`)
        // and projection (`withProjection`) if necessary
        Table table = GetTable.getTable("t1_db", "t1");

        PredicateBuilder builder =
            new PredicateBuilder(RowType.of(DataTypes.STRING(), DataTypes.INT()));
        Predicate notNull = builder.isNotNull(0);
        Predicate greaterOrEqual = builder.greaterOrEqual(1, 0);

        int[] projection = new int[] {0, 1};

        ReadBuilder readBuilder =
            table.newReadBuilder()
                .withProjection(projection)
                .withFilter(Lists.newArrayList(notNull, greaterOrEqual));

        // 2. Plan splits in 'Coordinator' (or named 'Driver')
        TableScan tableScan = readBuilder.newScan();
        TableScan.Plan plan = tableScan.plan();
        List<Split> splits = plan.splits();

        // 3. Distribute these splits to different tasks

        // 4. Read a split in task
        TableRead read = readBuilder.newRead();
        RecordReader<InternalRow> reader = read.createReader(splits);
        reader.forEachRemaining(k->{
            System.out.println(k.getString(0)+"\t"+k.getInt(1));
        });
    }
}