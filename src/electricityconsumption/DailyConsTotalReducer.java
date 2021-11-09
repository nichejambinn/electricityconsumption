package electricityconsumption;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class DailyConsTotalReducer extends Reducer<Text, DailyConsWritable, Text, DoubleWritable> {
    @Override
    protected void reduce(Text key, Iterable<DailyConsWritable> dailyConsIter, Context context) throws IOException, InterruptedException {
        double totalEnergyConsumption;
        double minEnergyReading = Double.MAX_VALUE;
        double maxEnergyReading = 0.0d;

        for (DailyConsWritable dailyCons: dailyConsIter) {
            minEnergyReading = Math.min(minEnergyReading, dailyCons.getMinEnergyReading());
            maxEnergyReading = Math.max(maxEnergyReading, dailyCons.getMaxEnergyReading());
        }

        totalEnergyConsumption = maxEnergyReading - minEnergyReading;

        context.write(key, new DoubleWritable(totalEnergyConsumption));
    }
}
