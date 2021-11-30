package electricityconsumption;

import electricityconsumption.daily.houseavg.*;
import electricityconsumption.hourly.housedatemax.HouseDateMaxDriver;

public class RunMapReduceJob {
    public static final String HDFS_URI = "hdfs://192.168.56.5:9820";
    
    public static void main(String[] args) throws Exception {
        new RunMapReduceJob().run(args);
    }

    public void run(String[] args) throws Exception {
        int task = Integer.parseInt(args[2]);
        switch (task) {
            case 1:
                System.out.println("Running maximum hourly energy consumption task");
                HouseDateMaxDriver.run(args);
                break;
            case 2:
                System.out.println("Running average daily energy consumption task");
                HouseAvgDriver.run(args);
                break;
            default:
                System.out.println("Invalid option");
                break;
        }
    }
}
