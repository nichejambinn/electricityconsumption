package electricityconsumption.daily;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class DailyTotalReducer extends Reducer<Text, DailyWritable, Text, DoubleWritable> {
    @Override
    protected void reduce(Text key, Iterable<DailyWritable> dailyConsIter, Context context) throws IOException, InterruptedException {
        double energyConsumption;
        double minEnergyReading = Double.MAX_VALUE;
        double maxEnergyReading = Double.MIN_VALUE;

        for (DailyWritable dailyCons: dailyConsIter) {
            minEnergyReading = Math.min(minEnergyReading, dailyCons.getMinEnergyReading());
            maxEnergyReading = Math.max(maxEnergyReading, dailyCons.getMaxEnergyReading());
        }

        energyConsumption = maxEnergyReading - minEnergyReading;

        context.write(key, new DoubleWritable(energyConsumption));
    }
}
