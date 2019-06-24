package simpledb;

import java.util.HashMap;
import java.util.Map;

/**
 * Knows how to compute some aggregate over a set of StringFields.
 */
public class StringAggregator implements Aggregator {

    private static final long serialVersionUID = 1L;

    /**
     * Aggregate constructor
     * @param gbfield the 0-based index of the group-by field in the tuple, or NO_GROUPING if there is no grouping
     * @param gbfieldtype the type of the group by field (e.g., Type.INT_TYPE), or null if there is no grouping
     * @param afield the 0-based index of the aggregate field in the tuple
     * @param what aggregation operator to use -- only supports COUNT
     * @throws IllegalArgumentException if what != COUNT
     */
    private int gbfield;
    private Type gbfieldtype;
    private int afield;
    private Op what;
    private Map<Field, Integer> countMap;
    private Map<Field, Tuple> groupMap;
    private TupleDesc td;
    public StringAggregator(int gbfield, Type gbfieldtype, int afield, Op what) {
        this.gbfield = gbfield;
        this.gbfieldtype = gbfieldtype;
        this.afield = afield;
        this.what = what;
        countMap = new HashMap<Field, Integer>();
        groupMap = new HashMap<Field, Tuple>();
        if(gbfield == NO_GROUPING){
            this.td = new TupleDesc(new Type[]{Type.INT_TYPE});
        }
        else{
            this.td = new TupleDesc(new Type[]{gbfieldtype, Type.INT_TYPE});
        }
        // some code goes here
    }

    /**
     * Merge a new tuple into the aggregate, grouping as indicated in the constructor
     * @param tup the Tuple containing an aggregate field and a group-by field
     */
    public void mergeTupleIntoGroup(Tuple tup) {
        // some code goes here
        Field key = gbfield == NO_GROUPING ? null: tup.getField(gbfield);
//        Integer value = ((StringField)tup.getField(afield)).getValue().length();


        if(countMap.containsKey(key)){
            countMap.put(key, countMap.get(key) + 1);
        }
        else{
            countMap.put(key, 1);
        }
        Integer aggValue = countMap.get(key);

        Tuple tmp = new Tuple(td);
        if(key == null){
            tmp.setField(0, new IntField(aggValue));
        }
        else{
            tmp.setField(0, key);
            tmp.setField(1, new IntField(aggValue));
        }
        groupMap.put(key, tmp);

    }

    /**
     * Create a DbIterator over group aggregate results.
     *
     * @return a DbIterator whose tuples are the pair (groupVal,
     *   aggregateVal) if using group, or a single (aggregateVal) if no
     *   grouping. The aggregateVal is determined by the type of
     *   aggregate specified in the constructor.
     */
    public DbIterator iterator() {
        // some code goes here

        return new TupleIterator(td, groupMap.values());
    }

}
