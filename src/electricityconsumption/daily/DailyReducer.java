package electricityconsumption.daily;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class DailyReducer extends Reducer<Text, DailyWritable, Text, DailyWritable> {
    @Override
    protected void reduce(Text key, Iterable<DailyWritable> dailyConsIter, Context context) throws IOException, InterruptedException {
        double minEnergyReading = Double.MAX_VALUE;
        double maxEnergyReading = Double.MIN_VALUE;

        for (DailyWritable dailyCons: dailyConsIter) {
            minEnergyReading = Math.min(minEnergyReading, dailyCons.getMinEnergyReading());
            maxEnergyReading = Math.max(maxEnergyReading, dailyCons.getMaxEnergyReading());
        }

        context.write(key, new DailyWritable(minEnergyReading, maxEnergyReading));
    }
}
