import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class PairTextDoubleWritable implements WritableComparable {
    public Text first;
    public DoubleWritable second;

    public PairTextDoubleWritable(){
        this.first = new Text("");
        this.second = new DoubleWritable(0);
    }

    public PairTextDoubleWritable(Text a, DoubleWritable b){
        this.first = a;
        this.second = b;

    }

    public Text first() {
        return first;
    }

    public DoubleWritable second() {
        return second;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        PairTextDoubleWritable that = (PairTextDoubleWritable) o;

        if (first != null ? !first.equals(that.first) : that.first != null) return false;
        if (second != null ? !second.equals(that.second) : that.second != null) return false;

        return true;
    }


    @Override
    public int compareTo(Object o) {
        PairTextDoubleWritable other = (PairTextDoubleWritable) o;
        int firstCompare = this.first.compareTo(other.first);
        if(firstCompare != 0) {
            return firstCompare;
        }
        return other.second.compareTo(this.second);
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

    @Override
    public String toString() {
        return first.toString() + "\t" + second.get();
    }
}
