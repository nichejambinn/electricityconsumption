package electricityconsumption.daily.houseavg;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class HouseAvgReducer extends Reducer<IntWritable, HouseWritable, IntWritable, DoubleWritable> {
    @Override
    protected void reduce(IntWritable key, Iterable<HouseWritable> houseConsIter, Context context) throws IOException, InterruptedException {
        double avgEnergyConsumption;
        double totalEnergyConsumption = 0.0d;
        int numDays = 0;

        for (HouseWritable houseCons: houseConsIter) {
            totalEnergyConsumption += houseCons.getTotalEnergyConsumption();
            numDays += houseCons.getNumDays();
        }

        avgEnergyConsumption = totalEnergyConsumption / numDays;
        context.write(key, new DoubleWritable(avgEnergyConsumption));
    }
}
