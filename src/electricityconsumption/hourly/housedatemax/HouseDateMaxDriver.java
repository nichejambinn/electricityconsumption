package electricityconsumption.hourly.housedatemax;

import electricityconsumption.RunMapReduceJob;
import electricityconsumption.hourly.HourlyMapper;
import electricityconsumption.hourly.HourlyReducer;
import electricityconsumption.hourly.HourlyTotalReducer;
import electricityconsumption.hourly.HourlyWritable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.net.URI;

import static electricityconsumption.RunMapReduceJob.HDFS_URI;

public class HouseDateMaxDriver {
    public static void run(String[] args) throws Exception {
        final String TMP_PATH = "/phase1-task1-tmp";

        Path inPath = new Path(args[0]);
        Path outPath = new Path(args[1]);

        // create a unique temporary intermediary output path for the first job
        String tempPathString = TMP_PATH + "-" + System.currentTimeMillis();
        Path tempPath = new Path(HDFS_URI + tempPathString);

        System.out.println(inPath);
        System.out.println(outPath);

        Configuration conf = new Configuration();

        // job 1: get total hourly energy consumption for each date and house
        Job job1 = Job.getInstance(conf);
        boolean job1success;
        job1.setJarByClass(RunMapReduceJob.class);

        job1.setJobName("Total Hourly Electricity Consumption");
        job1.setMapperClass(HourlyMapper.class);
        job1.setCombinerClass(HourlyReducer.class);
        job1.setReducerClass(HourlyTotalReducer.class);

        job1.setNumReduceTasks(4);

        // search directories recursively for input
        FileInputFormat.addInputPath(job1, inPath);
        FileInputFormat.setInputDirRecursive(job1, true);

        FileOutputFormat.setOutputPath(job1, tempPath);

        job1.setMapOutputValueClass(HourlyWritable.class);

        job1.setOutputKeyClass(Text.class);
        job1.setOutputValueClass(DoubleWritable.class);

        // job 2: get maximum hourly energy consumption for each date and house
        Job job2 = Job.getInstance(conf);
        boolean job2success;
        job2.setJarByClass(RunMapReduceJob.class);

        job2.setJobName("Maximum Hourly Electricity Consumption");
        job2.setMapperClass(HouseDateMapper.class);
        job2.setCombinerClass(HouseDateMaxReducer.class);
        job2.setReducerClass(HouseDateMaxReducer.class);

        job2.setNumReduceTasks(4);

        FileInputFormat.addInputPath(job2, tempPath);
        FileOutputFormat.setOutputPath(job2, outPath);

        job2.setOutputKeyClass(IntWritable.class);
        job2.setOutputValueClass(DoubleWritable.class);

        // chain job execution
        job1success = job1.waitForCompletion(true);
        if (job1success) {
            System.out.println("Job 1 successful");
            job2success = job2.waitForCompletion(true);
            if (job2success) {
                System.out.println("Job 2 successful");

                // remove temporary files
                Path tmp = new Path(tempPathString);
                FileSystem hdfs = FileSystem.get(URI.create(HDFS_URI), conf);
                if (hdfs.exists(tempPath)) {
                    hdfs.delete(tmp, true);
                }

                System.exit(0);
            } else {
                System.out.println("Job 2 failed");
                System.exit(1);
            }
        } else {
            System.out.println("Job 1 failed");
            System.exit(1);
        }
    }
}
