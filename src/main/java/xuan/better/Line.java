package xuan.better;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Author: liuxiao
 * Created: 2018/5/8 18:54
 * Description:
 */
public class Line implements WritableComparable<Line> {

    private String from;
    private String to;

    public Line() {
        this.from = "";
        this.to = "";
    }

    public Line(String from, String to) {
        this.from = from;
        this.to = to;
    }

    public String getFrom() {
        return from;
    }

    public void setFrom(String from) {
        this.from = from;
    }

    public String getTo() {
        return to;
    }

    public void setTo(String to) {
        this.to = to;
    }

    public int compareTo(Line o) {
        if (o == null) {
            return 1;
        }
        if (from.equals(o.getFrom())) {
            return to.compareTo(o.getTo());
        } else {
            return from.compareTo(o.getFrom());
        }
    }

    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeUTF(from);
        dataOutput.writeUTF(to);
    }

    public void readFields(DataInput dataInput) throws IOException {
        from = dataInput.readUTF();
        to = dataInput.readUTF();
    }
}
