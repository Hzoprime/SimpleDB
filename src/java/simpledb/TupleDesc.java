package simpledb;

import java.io.Serializable;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Stream;

/**
 * TupleDesc describes the schema of a tuple.
 */
public class TupleDesc implements Serializable {

    /**
     * A help class to facilitate organizing the information of each field
     */
    public static class TDItem implements Serializable {

        private static final long serialVersionUID = 1L;

        /**
         * The type of the field
         */
        public final Type fieldType;

        /**
         * The name of the field
         */
        public final String fieldName;

        public TDItem(Type t, String n) {
            this.fieldName = n;
            this.fieldType = t;
        }

        public String toString() {
            return fieldName + "(" + fieldType + ")";
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (!(o instanceof TDItem)) return false;
            TDItem tdItem = (TDItem) o;
            return fieldType == tdItem.fieldType &&
                    Objects.equals(fieldName, tdItem.fieldName);
        }

        @Override
        public int hashCode() {
            return Objects.hash(fieldType, fieldName);
        }
    }

    TDItem[] tdItems;

    /**
     * @return An iterator which iterates over all the field TDItems
     * that are included in this TupleDesc
     */
    public Iterator<TDItem> iterator() {
        // some code goes here
        return Arrays.stream(tdItems).iterator();
    }

    private static final long serialVersionUID = 1L;

    /**
     * Create a new TupleDesc with typeAr.length fields with fields of the
     * specified types, with associated named fields.
     *
     * @param typeAr
     * array specifying the number of and types of fields in this
     * TupleDesc. It must contain at least one entry.
     * @param fieldAr
     * array specifying the names of the fields. Note that names may
     * be null.
     */
    final static String DEFAULT_NAME = "column";

    public TupleDesc(Type[] typeAr, String[] fieldAr) {
        // some code goes here
        String[] fieldNames = fieldAr;
        if (fieldNames == null) {
//            AtomicInteger integer = new AtomicInteger();
//            fieldNames = Stream.generate(() -> DEFAULT_NAME + integer.getAndIncrement()).limit(typeAr.length).toArray(String[]::new);
//            fieldNames = Stream.generate(() -> null).limit(typeAr.length).toArray(String[]::new);
            fieldNames = new String[typeAr.length];
        }
        tdItems = new TDItem[typeAr.length];
        for (int i = 0; i < typeAr.length; i++) {
            tdItems[i] = new TDItem(typeAr[i], fieldNames[i]);
        }
    }

    /**
     * Constructor. Create a new tuple desc with typeAr.length fields with
     * fields of the specified types, with anonymous (unnamed) fields.
     *
     * @param typeAr array specifying the number of and types of fields in this
     *               TupleDesc. It must contain at least one entry.
     */

    public TupleDesc(Type[] typeAr) {
        // some code goes here
        AtomicInteger integer = new AtomicInteger();
//        String[] fieldNames = Stream.generate(() -> DEFAULT_NAME + (integer.getAndIncrement())).limit(typeAr.length).toArray(String[]::new);
        String[] fieldNames = new String[typeAr.length];
//        fieldNames = Stream.generate(() ->  (null)).limit(typeAr.length).toArray(String[]::new);
        tdItems = new TDItem[typeAr.length];
        for (int i = 0; i < typeAr.length; i++) {
            tdItems[i] = new TDItem(typeAr[i], fieldNames[i]);
        }
    }

    private TupleDesc() {

    }

    /**
     * @return the number of fields in this TupleDesc
     */
    public int numFields() {
        // some code goes here
        return tdItems.length;
    }

    /**
     * Gets the (possibly null) field name of the ith field of this TupleDesc.
     *
     * @param i index of the field name to return. It must be a valid index.
     * @return the name of the ith field
     * @throws NoSuchElementException if i is not a valid field reference.
     */
    public String getFieldName(int i) throws NoSuchElementException {
        // some code goes here
        return Objects.requireNonNullElseGet(tdItems[i].fieldName, () -> DEFAULT_NAME + i);
    }

    /**
     * Gets the type of the ith field of this TupleDesc.
     *
     * @param i The index of the field to get the type of. It must be a valid
     *          index.
     * @return the type of the ith field
     * @throws NoSuchElementException if i is not a valid field reference.
     */
    public Type getFieldType(int i) throws NoSuchElementException {
        // some code goes here
        return tdItems[i].fieldType;
    }

    /**
     * Find the index of the field with a given name.
     *
     * @param name name of the field.
     * @return the index of the field that is first to have the given name.
     * @throws NoSuchElementException if no field with a matching name is found.
     */
    public int fieldNameToIndex(String name) throws NoSuchElementException {
        // some code goes here
        for (int i = 0; i < numFields(); i++) {
            if (tdItems[i].fieldName.equals(name)) {
                return i;
            }
        }
        throw new NoSuchElementException();
    }

    /**
     * @return The size (in bytes) of tuples corresponding to this TupleDesc.
     * Note that tuples from a given TupleDesc are of a fixed size.
     */
    public int getSize() {
        // some code goes here
        return Arrays.stream(tdItems).map(tdItem -> tdItem.fieldType.getLen()).reduce(0, Integer::sum);
    }

    /**
     * Merge two TupleDescs into one, with td1.numFields + td2.numFields fields,
     * with the first td1.numFields coming from td1 and the remaining from td2.
     *
     * @param td1 The TupleDesc with the first fields of the new TupleDesc
     * @param td2 The TupleDesc with the last fields of the TupleDesc
     * @return the new TupleDesc
     */
    public static TupleDesc merge(TupleDesc td1, TupleDesc td2) {
        // some code goes here
        TupleDesc newTupleDesc = new TupleDesc();
        newTupleDesc.tdItems = new TDItem[td1.numFields() + td2.numFields()];
        System.arraycopy(td1.tdItems, 0, newTupleDesc.tdItems, 0, td1.numFields());
        System.arraycopy(td2.tdItems, 0, newTupleDesc.tdItems, td1.numFields(), td2.numFields());
        return newTupleDesc;
    }

    /**
     * Compares the specified object with this TupleDesc for equality. Two
     * TupleDescs are considered equal if they have the same number of items
     * and if the i-th type in this TupleDesc is equal to the i-th type in o
     * for every i.
     *
     * @param o the Object to be compared for equality with this TupleDesc.
     * @return true if the object is equal to this TupleDesc.
     */

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof TupleDesc)) return false;
        TupleDesc tupleDesc = (TupleDesc) o;
        return Arrays.equals(tdItems, tupleDesc.tdItems);
    }


    @Override
    public int hashCode() {
        // If you want to use TupleDesc as keys for HashMap, implement this so
        // that equal objects have equals hashCode() results
        return Objects.hashCode(tdItems);
    }

    /**
     * Returns a String describing this descriptor. It should be of the form
     * "fieldType[0](fieldName[0]), ..., fieldType[M](fieldName[M])", although
     * the exact format does not matter.
     *
     * @return String describing this descriptor.
     */
    public String toString() {
        // some code goes here
        String[] names;
        if (tdItems[0].fieldName == null) {
            AtomicInteger i = new AtomicInteger();
            names = Stream.generate(() -> DEFAULT_NAME + (i.getAndIncrement())).limit(tdItems.length).toArray(String[]::new);
        } else {
            names = Arrays.stream(tdItems).map(tdItem -> tdItem.fieldName).toArray(String[]::new);
        }
        StringBuilder builder = new StringBuilder();
        for (int i = 0; i < numFields(); i++) {
            builder.append(tdItems[i].fieldType).append("(").append(names[i]).append(")");
            if (i < numFields() - 1) {
                builder.append(",");
            }
        }
        return builder.toString();
    }
}
