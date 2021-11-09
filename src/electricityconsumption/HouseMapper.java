package electricityconsumption;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class HouseMapper extends Mapper<LongWritable, Text, IntWritable, HouseWritable> {
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] values = value.toString().split("\\s+");
        int houseId;
        double totalEnergyConsumption;

        try {
            houseId = Integer.parseInt(values[1]);
            totalEnergyConsumption = Double.parseDouble(values[2]);
        } catch (Exception ex) {
            houseId = -1;
            totalEnergyConsumption = 0.0d;
        }

        context.write(new IntWritable(houseId), new HouseWritable(totalEnergyConsumption, 1));
    }
}
