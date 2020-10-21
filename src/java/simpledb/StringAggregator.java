package simpledb;

import java.util.concurrent.ConcurrentHashMap;

/**
 * Knows how to compute some aggregate over a set of StringFields.
 */
public class StringAggregator implements Aggregator {

    private static final long serialVersionUID = 1L;

    /**
     * Aggregate constructor
     *
     * @param gbfield the 0-based index of the group-by field in the tuple, or NO_GROUPING if there is no grouping
     * @param gbfieldtype the type of the group by field (e.g., Type.INT_TYPE), or null if there is no grouping
     * @param afield the 0-based index of the aggregate field in the tuple
     * @param what aggregation operator to use -- only supports COUNT
     * @throws IllegalArgumentException if what != COUNT
     */
    int gbfield;
    Type gbfieldtype;
    int afield;
    Op what;

    ConcurrentHashMap<Field, Integer> countMap;
    static final Field noGroupingField = new IntField(-1);

    public StringAggregator(int gbfield, Type gbfieldtype, int afield, Op what) {
        // some code goes here
        if (what != Op.COUNT) {
            throw new IllegalArgumentException("what != COUNT");
        }
        this.afield = afield;
        this.gbfield = gbfield;
        this.gbfieldtype = gbfieldtype;
        countMap = new ConcurrentHashMap<>();
    }

    /**
     * Merge a new tuple into the aggregate, grouping as indicated in the constructor
     *
     * @param tup the Tuple containing an aggregate field and a group-by field
     */
    public void mergeTupleIntoGroup(Tuple tup) {
        // some code goes here
        Field gbField;
        if (this.gbfield == Aggregator.NO_GROUPING) {
            gbField = noGroupingField;
        } else {
            gbField = tup.getField(gbfield);
        }
        int oldCount = countMap.getOrDefault(gbField, 0);
        countMap.put(gbField, oldCount + 1);
    }

    /**
     * Create a OpIterator over group aggregate results.
     *
     * @return a OpIterator whose tuples are the pair (groupVal,
     * aggregateVal) if using group, or a single (aggregateVal) if no
     * grouping. The aggregateVal is determined by the type of
     * aggregate specified in the constructor.
     */
    public OpIterator iterator() {
        // some code goes here
        if (this.gbfield == NO_GROUPING) {

        } else {
            
        }
        throw new UnsupportedOperationException("please implement me for lab2");
    }

}
