package electricityconsumption.hourly.housedatemax;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class HouseDateMaxReducer extends Reducer<IntWritable, DoubleWritable, IntWritable, DoubleWritable> {
    @Override
    protected void reduce(IntWritable key, Iterable<DoubleWritable> houseDateIter, Context context) throws IOException, InterruptedException {
        double maxEnergyConsumption = Double.MIN_VALUE;

        for (DoubleWritable enerCons: houseDateIter) {
            maxEnergyConsumption = Math.max(maxEnergyConsumption, enerCons.get());
        }

        context.write(key, new DoubleWritable(maxEnergyConsumption));
    }
}
