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
        final String TMP_PATH = "hdfs://192.168.56.5:9820/phase1-tmp";

        Path inPath = new Path(args[0]);
        Path outPath = new Path(args[1]);

        // create a unique temporary intermediary output path for the first job
        String tempPathString = TMP_PATH + "-" + System.currentTimeMillis();
        Path tempPath = new Path(tempPathString);

        System.out.println(inPath);
        System.out.println(outPath);

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

        // search directories recursively for input
        FileInputFormat.addInputPath(job1, inPath);
        FileInputFormat.setInputDirRecursive(job1, true);

        FileOutputFormat.setOutputPath(job1, tempPath);

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

        System.exit(0);
    }
}
