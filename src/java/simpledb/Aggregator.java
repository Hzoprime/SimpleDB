package simpledb;

import java.io.Serializable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

/**
 * The common interface for any class that can compute an aggregate over a
 * list of Tuples.
 */
public interface Aggregator extends Serializable {
    int NO_GROUPING = -1;
    Field noGroupingField = new IntField(-1);

    /**
     * SUM_COUNT and SC_AVG will
     * only be used in lab7, you are not required
     * to implement them until then.
     */
    enum Op implements Serializable {
        MIN, MAX, SUM, AVG, COUNT,
        /**
         * SUM_COUNT: compute sum and count simultaneously, will be
         * needed to compute distributed avg in lab7.
         */
        SUM_COUNT,
        /**
         * SC_AVG: compute the avg of a set of SUM_COUNT tuples,
         * will be used to compute distributed avg in lab7.
         */
        SC_AVG;

        /**
         * Interface to access operations by a string containing an integer
         * index for command-line convenience.
         *
         * @param s a string containing a valid integer Op index
         */
        public static Op getOp(String s) {
            return getOp(Integer.parseInt(s));
        }

        /**
         * Interface to access operations by integer value for command-line
         * convenience.
         *
         * @param i a valid integer Op index
         */
        public static Op getOp(int i) {
            return values()[i];
        }

        public String toString() {
            if (this == MIN)
                return "min";
            if (this == MAX)
                return "max";
            if (this == SUM)
                return "sum";
            if (this == SUM_COUNT)
                return "sum_count";
            if (this == AVG)
                return "avg";
            if (this == COUNT)
                return "count";
            if (this == SC_AVG)
                return "sc_avg";
            throw new IllegalStateException("impossible to reach here");
        }
    }

    /**
     * Merge a new tuple into the aggregate for a distinct group value;
     * creates a new group aggregate result if the group value has not yet
     * been encountered.
     *
     * @param tup the Tuple containing an aggregate field and a group-by field
     */
    public void mergeTupleIntoGroup(Tuple tup);

    /**
     * Create a OpIterator over group aggregate results.
     *
     * @see simpledb.TupleIterator for a possible helper
     */
    public OpIterator iterator();

    static OpIterator getOpIterator(ConcurrentHashMap<Field, Integer> resultMap) {
        TupleDesc tupleDesc = new TupleDesc(new Type[]{Type.INT_TYPE});
        return new TupleIterator(
                tupleDesc,
                resultMap.values().stream()
                        .map(
                                integer -> {
                                    Tuple tuple = new Tuple(tupleDesc);
                                    tuple.setField(0, new IntField(integer));
                                    return tuple;
                                }
                        ).collect(Collectors.toList())
        );
    }

    static OpIterator getOpIterator(Type gbfieldtype, ConcurrentHashMap<Field, Integer> resultMap) {
        TupleDesc tupleDesc = new TupleDesc(new Type[]{gbfieldtype, Type.INT_TYPE});
        return new TupleIterator(
                tupleDesc,
                resultMap.entrySet().stream()
                        .map(
                                x -> {
                                    Tuple tuple = new Tuple(tupleDesc);
                                    tuple.setField(0, x.getKey());
                                    tuple.setField(1, new IntField(x.getValue()));
                                    return tuple;
                                }
                        ).collect(Collectors.toList())
        );
    }

    static OpIterator getOpIterator(Type gbfieldtype, ConcurrentHashMap<Field, Integer> resultMap, ConcurrentHashMap<Field, Integer> countMap) {
        System.out.println("sum count and sum count avg both are not in lab2");
        TupleDesc tupleDesc = new TupleDesc(new Type[]{gbfieldtype, Type.INT_TYPE, Type.INT_TYPE});
        return new TupleIterator(
                tupleDesc,
                resultMap.entrySet().stream()
                        .map(
                                x -> {
                                    Tuple tuple = new Tuple(tupleDesc);
                                    tuple.setField(0, x.getKey());
                                    tuple.setField(1, new IntField(x.getValue()));
                                    tuple.setField(2, new IntField(countMap.get(x.getKey())));
                                    return tuple;
                                }
                        ).collect(Collectors.toList())
        );
    }

}
