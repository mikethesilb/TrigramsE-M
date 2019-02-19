import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.WritableComparable;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class PairIntIntWritable implements WritableComparable {
    public IntWritable first;
    public IntWritable second;

    public PairIntIntWritable(){
        this.first = new IntWritable(0);
        this.second = new IntWritable(0);
    }

    public PairIntIntWritable(IntWritable a, IntWritable b){
        this.first = a;
        this.second = b;

    }

    public IntWritable first() {
        return first;
    }

    public IntWritable second() {
        return second;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        PairIntIntWritable that = (PairIntIntWritable) o;

        if (first != null ? !first.equals(that.first) : that.first != null) return false;
        if (second != null ? !second.equals(that.second) : that.second != null) return false;

        return true;
    }


    @Override
    public int compareTo(Object o) {
        PairIntIntWritable other = (PairIntIntWritable) o;
        int firstCompare = this.first.compareTo(other.first);
        if(firstCompare != 0) {
            return firstCompare;
        }
        return this.second.compareTo(other.second);
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        first.write(dataOutput);
        second.write(dataOutput);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        first.readFields(dataInput);
        //System.err.println("read key: " + first);
        second.readFields(dataInput);
        //System.err.println("read value: " + second);
    }

    public String toString() {
        return first.get() + "\t" + second.get();
    }
}
