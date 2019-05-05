package simpledb;

import java.io.Serializable;
import java.util.*;

/**
 * TupleDesc describes the schema of a tuple.
 */
public class TupleDesc implements Serializable {

    private ArrayList<TDItem> itemList;
    /**
     * A help class to facilitate organizing the information of each field
     * */
    public static class TDItem implements Serializable {



        private static final long serialVersionUID = 1L;

        /**
         * The type of the field
         * */
        public final Type fieldType;
        
        /**
         * The name of the field
         * */
        public final String fieldName;

        public TDItem(Type t, String n) {
            this.fieldName = n;
            this.fieldType = t;
        }

        public String toString() {
            return fieldName + "(" + fieldType + ")";
        }
    }

    /**
     * @return
     *        An iterator which iterates over all the field TDItems
     *        that are included in this TupleDesc
     * */
    public Iterator<TDItem> iterator() {
        return itemList.iterator();
        // some code goes here
    }

    private static final long serialVersionUID = 1L;

    /**
     * Create a new TupleDesc with typeAr.length fields with fields of the
     * specified types, with associated named fields.
     * 
     * @param typeAr
     *            array specifying the number of and types of fields in this
     *            TupleDesc. It must contain at least one entry.
     * @param fieldAr
     *            array specifying the names of the fields. Note that names may
     *            be null.
     */
    public TupleDesc(Type[] typeAr, String[] fieldAr) {
        int num = typeAr.length;
        itemList = new ArrayList<TDItem>();
        for (int i = 0; i < num; i++){
            TDItem TDitem = new TDItem(typeAr[i], fieldAr[i]);
            itemList.add(TDitem);
        }
        // some code goes here
    }

    /**
     * Constructor. Create a new tuple desc with typeAr.length fields with
     * fields of the specified types, with anonymous (unnamed) fields.
     * 
     * @param typeAr
     *            array specifying the number of and types of fields in this
     *            TupleDesc. It must contain at least one entry.
     */
    public TupleDesc(Type[] typeAr) {
        int num = typeAr.length;
        itemList = new ArrayList<TDItem>();
        for (int i = 0; i < num; i++){
            TDItem TDitem = new TDItem(typeAr[i], "");
            itemList.add(TDitem);
        }
        // some code goes here
    }

    /**
     * @return the number of fields in this TupleDesc
     */
    public int numFields() {
        // some code goes here
        return itemList.size();
    }

    /**
     * Gets the (possibly null) field name of the ith field of this TupleDesc.
     * 
     * @param i
     *            index of the field name to return. It must be a valid index.
     * @return the name of the ith field
     * @throws NoSuchElementException
     *             if i is not a valid field reference.
     */
    public String getFieldName(int i) throws NoSuchElementException {
        // some code goes here
        if (i < 0 || i >= itemList.size()){
            throw new NoSuchElementException("i is not a valid field reference");
        }
        else{
            return itemList.get(i).fieldName;
        }
    }

    /**
     * Gets the type of the ith field of this TupleDesc.
     * 
     * @param i
     *            The index of the field to get the type of. It must be a valid
     *            index.
     * @return the type of the ith field
     * @throws NoSuchElementException
     *             if i is not a valid field reference.
     */
    public Type getFieldType(int i) throws NoSuchElementException {
        // some code goes here
        if (i < 0 || i >= itemList.size()){
            throw new NoSuchElementException();
        }
        else{
            return itemList.get(i).fieldType;
        }
    }

    /**
     * Find the index of the field with a given name.
     * 
     * @param name
     *            name of the field.
     * @return the index of the field that is first to have the given name.
     * @throws NoSuchElementException
     *             if no field with a matching name is found.
     */
    public int fieldNameToIndex(String name) throws NoSuchElementException {
        // some code goes here
        if (name == null) throw new NoSuchElementException("");
        for (int i = 0; i < itemList.size(); ++i) {
            TDItem item = itemList.get(i);
            if (item.fieldName != null) {
                if (name.equals(item.fieldName)) {
                    return i;
                }
            }
        }
        throw new NoSuchElementException("no field with a matching name is found");
    }

    /**
     * @return The size (in bytes) of tuples corresponding to this TupleDesc.
     *         Note that tuples from a given TupleDesc are of a fixed size.
     */
    public int getSize() {
        // some code goes here
        int size = 0;
        for (TDItem item: itemList){
            size += item.fieldType.getLen();
        }
        return size;
    }

    /**
     * Merge two TupleDescs into one, with td1.numFields + td2.numFields fields,
     * with the first td1.numFields coming from td1 and the remaining from td2.
     * 
     * @param td1
     *            The TupleDesc with the first fields of the new TupleDesc
     * @param td2
     *            The TupleDesc with the last fields of the TupleDesc
     * @return the new TupleDesc
     */
    public static TupleDesc merge(TupleDesc td1, TupleDesc td2) {
        // some code goes here
        int counter = td1.numFields();
        int all_num = counter + td2.numFields();
        Type all_type[] = new Type[all_num];
        String all_name[] = new String[all_num];

        for (int i = 0; i < td1.numFields(); ++i) {
            all_type[i] = td1.getFieldType(i);
            all_name[i] = td1.getFieldName(i);
        }

        for (int i = 0; i < td2.numFields(); ++i) {
            all_type[i + counter] = td2.getFieldType(i);
            all_name[i + counter] = td2.getFieldName(i);
        }

        return new TupleDesc(all_type, all_name);
    }

    /**
     * Compares the specified object with this TupleDesc for equality. Two
     * TupleDescs are considered equal if they are the same size and if the n-th
     * type in this TupleDesc is equal to the n-th type in td.
     * 
     * @param o
     *            the Object to be compared for equality with this TupleDesc.
     * @return true if the object is equal to this TupleDesc.
     */
    public boolean equals(Object o) {
        if (o instanceof TupleDesc){
            TupleDesc tupleDesc = (TupleDesc)o;
            if (tupleDesc.numFields() == this.numFields()){
                for (int i = 0; i < tupleDesc.numFields(); ++i) {
                    if (tupleDesc.getFieldType(i) != this.getFieldType(i)){
                        return false;
                    }
                }
                return true;
            }
        }
        return false;
        // some code goes here
    }

    public int hashCode() {
        // If you want to use TupleDesc as keys for HashMap, implement this so
        // that equal objects have equals hashCode() results
        throw new UnsupportedOperationException("unimplemented");
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
        String string = "";
        for (TDItem item: itemList){
            string += item.fieldType.toString() + "(" + item.fieldName + "), ";
        }
        string = string.substring(0, string.length() - 2);
        return string;
    }
}
