package electricityconsumption.daily;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class DailyMapper extends Mapper<LongWritable, Text, Text, DailyWritable> {
    @Override
    protected void map(LongWritable key, Text value, Mapper.Context context) throws IOException, InterruptedException {
        String[] values = value.toString().split(",");
        String date;
        int houseId;
        double energyReading;

        try {
            houseId = Integer.parseInt(values[1]);
            date = values[2];
            energyReading = Double.parseDouble(values[4]);
        } catch (Exception ex) {
            houseId = -1;
            date = "9999-99-99";
            energyReading = 0.0d;
        }

        context.write(new Text(date + "\t" + houseId), new DailyWritable(energyReading, energyReading));
    }
}
