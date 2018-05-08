package xuan.better;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Author: liuxiao
 * Created: 2018/5/8 19:32
 * Description:
 */
@Deprecated
public class LineResult implements WritableComparable<LineResult> {

    private int count;
    private double sumDistance;

    public LineResult() {
    }

    public LineResult(int count, double sumDistance) {
        this.count = count;
        this.sumDistance = sumDistance;
    }

    public int getCount() {
        return count;
    }

    public void setCount(int count) {
        this.count = count;
    }

    public double getSumDistance() {
        return sumDistance;
    }

    public void setSumDistance(double sumDistance) {
        this.sumDistance = sumDistance;
    }

    public int compareTo(LineResult o) {
        if (o == null) {
            return 1;
        }
        if (count == o.getCount()) {
            return (int)(sumDistance - o.getSumDistance());
        } else {
            return count - o.getCount();
        }
    }

    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeInt(count);
        dataOutput.writeDouble(sumDistance);
    }

    public void readFields(DataInput dataInput) throws IOException {
        count = dataInput.readInt();
        sumDistance = dataInput.readDouble();
    }
}
