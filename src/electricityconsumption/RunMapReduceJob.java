package electricityconsumption;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class RunMapReduceJob {
    public static void main(String[] args) throws Exception {
        new RunMapReduceJob().run(args);
    }

    public void run(String[] args) throws Exception {
        Path inPath = new Path(args[0]);
        Path outPath = new Path(args[1]);

        System.out.println(inPath.toString());
        System.out.println(outPath.toString());

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);
        job.setJarByClass(RunMapReduceJob.class);

        // part 1: get total energy consumption for each date and house
        job.setJobName("Total Daily Electricity Consumption");
        boolean part1success, part2success;
        job.setMapperClass(DailyConsMapper.class);
        job.setCombinerClass(DailyConsReducer.class);
        job.setReducerClass(DailyConsTotalReducer.class);

        job.setNumReduceTasks(4);

        FileInputFormat.addInputPath(job, inPath);
        FileOutputFormat.setOutputPath(job, outPath);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(DoubleWritable.class);

        part1success = job.waitForCompletion(true);
        if (part1success) {

        } else {
            System.exit(part1success ? 0 : 1);
        }
    }
}
