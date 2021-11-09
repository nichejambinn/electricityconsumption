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
        
        // job 1: get total energy consumption for each date and house
        Job job1 = Job.getInstance(conf);
        boolean job1success;
        job1.setJarByClass(RunMapReduceJob.class);

        job1.setJobName("Total Daily Electricity Consumption");
        job1.setMapperClass(DailyConsMapper.class);
        job1.setCombinerClass(DailyConsReducer.class);
        job1.setReducerClass(DailyConsTotalReducer.class);

        job1.setNumReduceTasks(4);

        FileInputFormat.addInputPath(job1, inPath);
        FileOutputFormat.setOutputPath(job1, outPath);

        job1.setMapOutputValueClass(DailyConsWritable.class);

        job1.setOutputKeyClass(Text.class);
        job1.setOutputValueClass(DoubleWritable.class);

//        // job 2: get average daily energy consumption for each house
//        Job job2 = Job.getInstance(conf);
//        boolean job2success;
//        job2.setJarByClass(RunMapReduceJob.class);
//        job2.setJobName("Average Daily Electricity Consumption");

        job1success = job1.waitForCompletion(true);
        if (job1success) {
            System.out.println("Job 1 successful");
        } else {
            System.out.println("Job 1 failed");
            System.exit(1);
        }
    }
}
