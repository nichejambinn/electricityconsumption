package electricityconsumption;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class HouseWritable implements Writable {
    private double totalEnergyConsumption;
    private int numDays;

    HouseWritable() {
        this.totalEnergyConsumption = 0;
        this.numDays = 0;
    }

    HouseWritable(double totalEnergyReading, int numDays) {
        this.totalEnergyConsumption = totalEnergyReading;
        this.numDays = numDays;
    }

    public double getTotalEnergyConsumption() {
        return totalEnergyConsumption;
    }

    public int getNumDays() {
        return numDays;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeDouble(totalEnergyConsumption);
        out.writeInt(numDays);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        totalEnergyConsumption = in.readDouble();
        numDays = in.readInt();
    }

    public static HouseWritable read(DataInput in) throws IOException {
        HouseWritable w = new HouseWritable();
        w.readFields(in);
        return w;
    }
}
