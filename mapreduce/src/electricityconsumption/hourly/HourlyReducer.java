package electricityconsumption.hourly;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class HourlyReducer extends Reducer<Text, HourlyWritable, Text, HourlyWritable> {
    @Override
    protected void reduce(Text key, Iterable<HourlyWritable> dailyConsIter, Context context) throws IOException, InterruptedException {
        double minEnergyReading = Double.MAX_VALUE;
        double maxEnergyReading = Double.MIN_VALUE;

        for (HourlyWritable dailyCons: dailyConsIter) {
            minEnergyReading = Math.min(minEnergyReading, dailyCons.getMinEnergyReading());
            maxEnergyReading = Math.max(maxEnergyReading, dailyCons.getMaxEnergyReading());
        }

        context.write(key, new HourlyWritable(minEnergyReading, maxEnergyReading));
    }
}
