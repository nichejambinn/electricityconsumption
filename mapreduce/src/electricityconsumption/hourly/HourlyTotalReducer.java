package electricityconsumption.hourly;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class HourlyTotalReducer extends Reducer<Text, HourlyWritable, Text, DoubleWritable> {
    @Override
    protected void reduce(Text key, Iterable<HourlyWritable> dailyConsIter, Context context) throws IOException, InterruptedException {
        double energyConsumption;
        double minEnergyReading = Double.MAX_VALUE;
        double maxEnergyReading = Double.MIN_VALUE;

        for (HourlyWritable dailyCons: dailyConsIter) {
            minEnergyReading = Math.min(minEnergyReading, dailyCons.getMinEnergyReading());
            maxEnergyReading = Math.max(maxEnergyReading, dailyCons.getMaxEnergyReading());
        }

        energyConsumption = maxEnergyReading - minEnergyReading;

        context.write(key, new DoubleWritable(energyConsumption));
    }
}
