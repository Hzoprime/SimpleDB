package simpledb;

import java.io.File;

public class TestLab1 {
    public static void main(String[] args) {
        Type[] types = new Type[]{Type.INT_TYPE, Type.INT_TYPE, Type.INT_TYPE};
        String[] names = new String[]{"field0", "field1", "field2"};
        TupleDesc descriptor = new TupleDesc(types, names);
        HeapFile table1 = new HeapFile(new File("test_data.dat"), descriptor);
        Database.getCatalog().addTable(table1, "test");
        TransactionId tid = new TransactionId();
        SeqScan f = new SeqScan(tid, table1.getId());
        try {
            f.open();
            while (f.hasNext()) {
                Tuple tup = f.next();
                System.out.println(tup);
            }
            f.close();
            Database.getBufferPool().transactionComplete(tid);
        } catch (Exception e) {
            System.out.println("Exception : " + e);
        }
    }
}
