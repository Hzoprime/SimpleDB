package simpledb;

import java.util.Arrays;
import java.util.Comparator;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

/**
 * Knows how to compute some aggregate over a set of IntFields.
 */
public class IntegerAggregator implements Aggregator {

    private static final long serialVersionUID = 1L;

    /**
     * Aggregate constructor
     *
     * @param gbfield
     * the 0-based index of the group-by field in the tuple, or
     * NO_GROUPING if there is no grouping
     * @param gbfieldtype
     * the type of the group by field (e.g., Type.INT_TYPE), or null
     * if there is no grouping
     * @param afield
     * the 0-based index of the aggregate field in the tuple
     * @param what
     * the aggregation operator
     */
    int gbfield;
    Type gbfieldtype;
    int afield;
    Op what;

    ConcurrentHashMap<Field, Integer> valueMap;
    ConcurrentHashMap<Field, Integer> countMap;

    static final Field noGroupingField = new IntField(-1);

    public IntegerAggregator(int gbfield, Type gbfieldtype, int afield, Op what) {
        // some code goes here
        this.gbfield = gbfield;
        this.gbfieldtype = gbfieldtype;
        this.afield = afield;
        this.what = what;
        valueMap = new ConcurrentHashMap<>();
        countMap = new ConcurrentHashMap<>();
    }

    /**
     * Merge a new tuple into the aggregate, grouping as indicated in the
     * constructor
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

        Integer oldValue = valueMap.get(gbField);
        int currentValue = ((IntField) tup.getField(afield)).getValue();
        valueMap.put(gbField, getNewValue(oldValue, currentValue));
    }

    private int getNewValue(Integer oldValue, int newValue) {
        if (oldValue == null) {
            return newValue;
        }
        switch (what) {
            case MIN:
                return Math.min(oldValue, newValue);
            case MAX:
                return Math.max(oldValue, newValue);
            case SUM:
            case SC_AVG:
            case SUM_COUNT:
            case AVG:
                return oldValue + newValue;
            case COUNT:
                return oldValue + 1;
        }
        return 0;
    }

    /**
     * Create a OpIterator over group aggregate results.
     *
     * @return a OpIterator whose tuples are the pair (groupVal, aggregateVal)
     * if using group, or a single (aggregateVal) if no grouping. The
     * aggregateVal is determined by the type of aggregate specified in
     * the constructor.
     */
    public OpIterator iterator() {
        // some code goes here
        // TODO: 2020/10/20 avg should be double, but tuple just suit integer
        ConcurrentHashMap<Field, Integer> resultMap = new ConcurrentHashMap<>();
        if (this.what == Op.AVG) {
            for (Field field : countMap.keySet()) {
                int oldValue = valueMap.get(field);
                int count = countMap.get(field);
                int newValue = oldValue / count;
                resultMap.put(field, newValue);
            }
        } else {
            resultMap = valueMap;
        }

        if (this.gbfield == Aggregator.NO_GROUPING) {
            TupleDesc tupleDesc = new TupleDesc(new Type[]{Type.INT_TYPE});
            return new TupleIterator(tupleDesc,
                    resultMap.values().stream().map(
                            integer -> {
                                Tuple tuple = new Tuple(tupleDesc);
                                tuple.setField(0, new IntField(integer));
                                return tuple;
                            }
                    ).collect(Collectors.toList()));
        } else if (this.what == Op.SC_AVG || this.what == Op.SUM_COUNT) {
            System.out.println("not in lab2");
            return null;
        } else {
            TupleDesc tupleDesc = new TupleDesc(new Type[]{this.gbfieldtype, Type.INT_TYPE});
            return new TupleIterator(
                    tupleDesc,
                    resultMap.entrySet().stream().sorted(
                            Comparator.comparingDouble(Map.Entry::getValue)
                    ).map(
                            x -> {
                                Tuple tuple = new Tuple(tupleDesc);
                                tuple.setField(0, x.getKey());
                                tuple.setField(1, new IntField(x.getValue()));
                                return tuple;
                            }
                    ).collect(Collectors.toList())
            );
        }
    }
}