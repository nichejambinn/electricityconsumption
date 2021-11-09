package electricityconsumption;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class DailyConsMapper extends Mapper<LongWritable, Text, Text, DailyConsWritable> {
    @Override
    protected void map(LongWritable key, Text value, Mapper.Context context) throws IOException, InterruptedException {
        String[] values = value.toString().split("\\s+");
        String date;
        int houseId;
        double energyReading;

        try {
            houseId = Integer.parseInt(values[1]);
            date = values[2];
            energyReading = Double.parseDouble(values[3]);
        } catch (Exception ex) {
            houseId = -1;
            date = "9999-99-99";
            energyReading = 0.0d;
        }

        context.write(new Text(date + "\t" + houseId), new DailyConsWritable(energyReading, energyReading));
    }
}
