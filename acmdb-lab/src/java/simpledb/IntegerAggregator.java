package simpledb;

import java.util.HashMap;
import java.util.Map;

/**
 * Knows how to compute some aggregate over a set of IntFields.
 */
public class IntegerAggregator implements Aggregator {

    private static final long serialVersionUID = 1L;

    /**
     * Aggregate constructor
     * 
     * @param gbfield
     *            the 0-based index of the group-by field in the tuple, or
     *            NO_GROUPING if there is no grouping
     * @param gbfieldtype
     *            the type of the group by field (e.g., Type.INT_TYPE), or null
     *            if there is no grouping
     * @param afield
     *            the 0-based index of the aggregate field in the tuple
     * @param what
     *            the aggregation operator
     */
    private int gbfield;
    private Type gbfieldtype;
    private int afield;
    private Op what;
    private Map<Field, Integer> maxMap, minMap, sumMap, countMap;
    private Map<Field, Tuple> groupMap;
    private TupleDesc td;

    public IntegerAggregator(int gbfield, Type gbfieldtype, int afield, Op what) {
        this.gbfield = gbfield;
        this.gbfieldtype = gbfieldtype;
        this.afield = afield;
        this.what = what;
        groupMap = new HashMap<Field, Tuple>();
        switch(what){
            case MIN:
                minMap = new HashMap<Field, Integer>();
                break;
            case MAX:
                maxMap = new HashMap<Field, Integer>();
                break;
            case SUM:
                sumMap = new HashMap<Field, Integer>();
                break;
            case AVG:
                sumMap = new HashMap<Field, Integer>();
                countMap = new HashMap<Field, Integer>();
            case COUNT:
                countMap = new HashMap<Field, Integer>();
                break;
            case SUM_COUNT:
                break;
            case SC_AVG:
                break;
        }

        if (this.gbfield == NO_GROUPING) {
            this.td = new TupleDesc(new Type[]{Type.INT_TYPE});
        } else {
            this.td = new TupleDesc(new Type[]{gbfieldtype, Type.INT_TYPE});
        }
        // some code goes here
    }

    /**
     * Merge a new tuple into the aggregate, grouping as indicated in the
     * constructor
     * 
     * @param tup
     *            the Tuple containing an aggregate field and a group-by field
     */
    public void mergeTupleIntoGroup(Tuple tup) {
        Field key = gbfield == NO_GROUPING ? null : tup.getField(gbfield);
        Integer value = ((IntField)tup.getField(afield)).getValue();

        Integer aggValue = 0;

        switch (what){
            case MIN:
                if(minMap.containsKey(key)){
                    minMap.put(key, Math.min(value, minMap.get(key)));
                }
                else{
                    minMap.put(key, value);
                }
                aggValue = minMap.get(key);
                break;
            case MAX:
                if(maxMap.containsKey(key)){
                    maxMap.put(key, Math.max(value, maxMap.get(key)));
                }
                else{
                    maxMap.put(key, value);
                }
                aggValue = maxMap.get(key);
                break;
            case SUM:
                if(sumMap.containsKey(key)){
                    sumMap.put(key, value + sumMap.get(key));
                }
                else{
                    sumMap.put(key, value);
                }
                aggValue = sumMap.get(key);
                break;
            case AVG:
                if(sumMap.containsKey(key) && countMap.containsKey(key)){
                    sumMap.put(key, value + sumMap.get(key));
                    countMap.put(key, countMap.get(key) + 1);
                }
                else{
                    sumMap.put(key, value);
                    countMap.put(key, 1);
                }
                aggValue = sumMap.get(key) / countMap.get(key);
                break;
            case COUNT:
                if(countMap.containsKey(key)){
                    countMap.put(key, countMap.get(key) + 1);
                }
                else{
                    countMap.put(key, 1);
                }
                aggValue = countMap.get(key);
                break;
            case SUM_COUNT:
                break;
            case SC_AVG:
                break;
        }

        Tuple tmp = new Tuple(td);

        if(key == null) {
            tmp.setField(0, new IntField(aggValue));
        }
        else{
            tmp.setField(0, key);
            tmp.setField(1, new IntField(aggValue));
        }
        groupMap.put(key, tmp);
        // some code goes here
    }

    /**
     * Create a DbIterator over group aggregate results.
     * 
     * @return a DbIterator whose tuples are the pair (groupVal, aggregateVal)
     *         if using group, or a single (aggregateVal) if no grouping. The
     *         aggregateVal is determined by the type of aggregate specified in
     *         the constructor.
     */
    public DbIterator iterator() {
        // some code goes here
        return new TupleIterator(td, groupMap.values());
    }

}
