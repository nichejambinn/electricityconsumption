package electricityconsumption;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class DailyConsReducer extends Reducer<Text, DailyConsWritable, Text, DailyConsWritable> {
    @Override
    protected void reduce(Text key, Iterable<DailyConsWritable> dailyConsIter, Context context) throws IOException, InterruptedException {
        double minEnergyReading = 0.0d;
        double maxEnergyReading = 0.0d;

        for (DailyConsWritable dailyCons: dailyConsIter) {
            minEnergyReading = Math.min(minEnergyReading, dailyCons.getMinEnergyReading());
            maxEnergyReading = Math.max(maxEnergyReading, dailyCons.getMaxEnergyReading());
        }

        context.write(key, new DailyConsWritable(minEnergyReading, maxEnergyReading));
    }
}
