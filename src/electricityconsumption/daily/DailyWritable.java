package electricityconsumption.daily;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class DailyWritable implements Writable {
    private double minEnergyReading;
    private double maxEnergyReading;

    DailyWritable() {
        this.minEnergyReading = 0;
        this.maxEnergyReading = 0;
    }

    DailyWritable(double minEnergyReading, double maxEnergyReading) {
        this.minEnergyReading = minEnergyReading;
        this.maxEnergyReading = maxEnergyReading;
    }

    public double getMinEnergyReading() {
        return minEnergyReading;
    }

    public double getMaxEnergyReading() {
        return maxEnergyReading;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeDouble(minEnergyReading);
        out.writeDouble(maxEnergyReading);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        minEnergyReading = in.readDouble();
        maxEnergyReading = in.readDouble();
    }

    public static DailyWritable read(DataInput in) throws IOException {
        DailyWritable w = new DailyWritable();
        w.readFields(in);
        return w;
    }
}
