package electricityconsumption.hourly.housedatemax;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class HouseDateMapper extends Mapper<LongWritable, Text, IntWritable, DoubleWritable> {
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] values = value.toString().split("\\s+");
        int houseId;
        double totalEnergyConsumption;

        try {
            houseId = Integer.parseInt(values[2]);
            totalEnergyConsumption = Double.parseDouble(values[3]);
        } catch (Exception ex) {
            houseId = -1;
            totalEnergyConsumption = 0.0d;
        }

        context.write(new IntWritable(houseId), new DoubleWritable(totalEnergyConsumption));
    }
}
