package electricityconsumption.hourly;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class HourlyMapper extends Mapper<LongWritable, Text, Text, HourlyWritable> {
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] values = value.toString().split(",");
        String date;
        int hour;
        int houseId;
        double energyReading;

        try {
            houseId = Integer.parseInt(values[1]);
            date = values[2];
            hour = Integer.parseInt(values[3].split(":")[0]);
            energyReading = Double.parseDouble(values[4]);
        } catch (Exception ex) {
            houseId = -1;
            date = "9999-99-99";
            hour = 0;
            energyReading = 0.0d;
        }

        context.write(new Text(date + "\t" + hour + "\t" + houseId), new HourlyWritable(energyReading, energyReading));
    }
}
