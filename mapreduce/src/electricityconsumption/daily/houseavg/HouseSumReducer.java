package electricityconsumption.daily.houseavg;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class HouseSumReducer extends Reducer<IntWritable, HouseWritable, IntWritable, HouseWritable> {
    @Override
    protected void reduce(IntWritable key, Iterable<HouseWritable> houseConsIter, Context context) throws IOException, InterruptedException {
        double totalEnergyConsumption = 0.0d;
        int numDays = 0;

        for (HouseWritable houseCons: houseConsIter) {
            totalEnergyConsumption += houseCons.getTotalEnergyConsumption();
            numDays += houseCons.getNumDays();
        }

        context.write(key, new HouseWritable(totalEnergyConsumption, numDays));
    }
}
