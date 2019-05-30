package simpledb;

/** A class to represent a fixed-width histogram over a single integer-based field.
 */
public class IntHistogram {

    /**
     * Create a new IntHistogram.
     * 
     * This IntHistogram should maintain a histogram of integer values that it receives.
     * It should split the histogram into "buckets" buckets.
     * 
     * The values that are being histogrammed will be provided one-at-a-time through the "addValue()" function.
     * 
     * Your implementation should use space and have execution time that are both
     * constant with respect to the number of values being histogrammed.  For example, you shouldn't 
     * simply store every value that you see in a sorted list.
     * 
     * @param buckets The number of buckets to split the input value into.
     * @param min The minimum integer value that will ever be passed to this class for histogramming
     * @param max The maximum integer value that will ever be passed to this class for histogramming
     */

    int buckets;
    int min;
    int max;
    int tuples;
    int width;
    int [] histogram;


    public IntHistogram(int buckets, int min, int max) {

        this.buckets = buckets;
        this.min = min;
        this.max = max;
        this.width = (int)Math.ceil(((double)(max - min + 1)) / buckets);
        this.tuples = 0;
        this.histogram = new int [buckets];
        for (int i = 0; i < buckets; ++i) {
            this.histogram[i] = 0;
        }
    	// some code goes here
    }

    /**
     * Add a value to the set of values that you are keeping a histogram of.
     * @param v Value to add to the histogram
     */
    public void addValue(int v) {
        int position = (v - this.min) / width;
        histogram[position] += 1;
        tuples += 1;
    	// some code goes here
    }

    /**
     * Estimate the selectivity of a particular predicate and operand on this table.
     * 
     * For example, if "op" is "GREATER_THAN" and "v" is 5, 
     * return your estimate of the fraction of elements that are greater than 5.
     * 
     * @param op Operator
     * @param v Value
     * @return Predicted selectivity of this particular operator and value
     */
    public double estimateSelectivity(Predicate.Op op, int v) {
        int index = (v - this.min) / width;
        int left = min + index * width;
        int right = min + left + width;
        double prob = 0.0;
        switch (op){
            case EQUALS:
                if(v > max || v < min){
                    return 0.0;
                }
//                System.out.println((double)histogram[index] / width / tuples);
                return ((double)histogram[index] / width / tuples);
            case GREATER_THAN:
                if(v >= max){
                    return 0.0;
                }
                if(v < min){
                    return 1.0;
                }


                prob = ((double) (right - v) / width * histogram[index]) / tuples;

                for (int i = index + 1; i < this.buckets; ++i){
                    prob += (double)histogram[i] /tuples;
                }
                return prob;
            case LESS_THAN:
                if(v <= this.min){
                    return 0.0;
                }
                if(v > this.max){
                    return 1.0;
                }

                prob = (double)(v - left) / width * histogram[index] / tuples;

                for (int i = 0; i < index; ++i) {
                    prob += (double)histogram[i] / tuples;
                }
                return prob;


            case LESS_THAN_OR_EQ:
                if(v < this.min){
                    return 0.0;
                }
                if(v >= this.max){
                    return 1.0;
                }

                prob = (double)(v + 1 - left) / width * histogram[index] / tuples;

                for (int i = 0; i < index; ++i) {
                    prob += (double)histogram[i] / tuples;
                }
                return prob;

            case GREATER_THAN_OR_EQ:
                if(v > max){
                    return 0.0;
                }
                if(v <= min){
                    return 1.0;
                }


                prob = ((double) (right - v) / width * histogram[index]) / tuples;

                for (int i = index + 1; i < this.buckets; ++i){
                    prob += (double)histogram[i] /tuples;
                }
                return prob;

            case LIKE:
                if (v < min || v > max) {
                    return 0.0;
                }

                return (double)histogram[index] / width / tuples;

            case NOT_EQUALS:
                if(v > max || v < min){
                    return 1.0;
                }
                return (1 - (double)histogram[index] / width / tuples);
        }
    	// some code goes here
        return -1.0;
    }
    
    /**
     * @return
     *     the average selectivity of this histogram.
     *     
     *     This is not an indispensable method to implement the basic
     *     join optimization. It may be needed if you want to
     *     implement a more efficient optimization
     * */
    public double avgSelectivity()
    {

        // some code goes here
        return 1.0;
    }
    
    /**
     * @return A string describing this histogram, for debugging purposes
     */
    public String toString() {
        // some code goes here
        return null;
    }
}
