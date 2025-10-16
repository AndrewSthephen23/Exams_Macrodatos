

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.mapred.lib.MultipleInputs;

public class EnergyDriver {

    public static void main(String[] args) {
        JobClient my_client = new JobClient();

        // Job 1: Agregar energía total por día
        JobConf job_conf1 = new JobConf(EnergyDriver.class);
        job_conf1.setJobName("EnergyJob1_DailyAggregation");
        job_conf1.setOutputKeyClass(Text.class);
        job_conf1.setOutputValueClass(Text.class);
        job_conf1.setMapperClass(EnergyMapper.class);
        job_conf1.setReducerClass(EnergyReducer.class);
        job_conf1.setInputFormat(TextInputFormat.class);
        job_conf1.setOutputFormat(TextOutputFormat.class);

        FileInputFormat.setInputPaths(job_conf1, new Path(args[0]));
        FileOutputFormat.setOutputPath(job_conf1, new Path(args[1] + "/temp1"));
        my_client.setConf(job_conf1);

        // Job 2: Calcular promedio de energía diaria del año
        JobConf job_conf2 = new JobConf(EnergyDriver.class);
        job_conf2.setJobName("EnergyJob2_AnnualAverage");
        job_conf2.setOutputKeyClass(Text.class);
        job_conf2.setOutputValueClass(Text.class);
        job_conf2.setMapperClass(EnergyMapper2.class);
        job_conf2.setReducerClass(EnergyReducer2.class);
        job_conf2.setInputFormat(TextInputFormat.class);
        job_conf2.setOutputFormat(TextOutputFormat.class);

        FileInputFormat.setInputPaths(job_conf2, new Path(args[1] + "/temp1"));
        FileOutputFormat.setOutputPath(job_conf2, new Path(args[1] + "/temp2"));

        // Job 3: Preparar datos del promedio para join
        JobConf job_conf3 = new JobConf(EnergyDriver.class);
        job_conf3.setJobName("EnergyJob3_PrepareAverage");
        job_conf3.setOutputKeyClass(Text.class);
        job_conf3.setOutputValueClass(Text.class);
        job_conf3.setMapperClass(EnergyMapper3.class);
        job_conf3.setReducerClass(PassThroughReducer.class);
        job_conf3.setInputFormat(TextInputFormat.class);
        job_conf3.setOutputFormat(TextOutputFormat.class);

        FileInputFormat.setInputPaths(job_conf3, new Path(args[1] + "/temp2"));
        FileOutputFormat.setOutputPath(job_conf3, new Path(args[1] + "/temp3"));

        // Job 4: Preparar datos diarios para join
        JobConf job_conf4 = new JobConf(EnergyDriver.class);
        job_conf4.setJobName("EnergyJob4_PrepareDays");
        job_conf4.setOutputKeyClass(Text.class);
        job_conf4.setOutputValueClass(Text.class);
        job_conf4.setMapperClass(EnergyMapper4.class);
        job_conf4.setReducerClass(PassThroughReducer.class);
        job_conf4.setInputFormat(TextInputFormat.class);
        job_conf4.setOutputFormat(TextOutputFormat.class);

        FileInputFormat.setInputPaths(job_conf4, new Path(args[1] + "/temp1"));
        FileOutputFormat.setOutputPath(job_conf4, new Path(args[1] + "/temp4"));

        // Job 5: Clasificar y contar días de alta energía
        JobConf job_conf5 = new JobConf(EnergyDriver.class);
        job_conf5.setJobName("EnergyJob5_ClassifyAndCount");
        job_conf5.setOutputKeyClass(Text.class);
        job_conf5.setOutputValueClass(Text.class);
        job_conf5.setMapperClass(PassThroughMapper.class);
        job_conf5.setReducerClass(EnergyReducer3.class);
        job_conf5.setInputFormat(TextInputFormat.class);
        job_conf5.setOutputFormat(TextOutputFormat.class);

        FileInputFormat.setInputPaths(job_conf5, new Path(args[1] + "/temp3"), new Path(args[1] + "/temp4"));
        FileOutputFormat.setOutputPath(job_conf5, new Path(args[1] + "/final"));

        try {
            JobClient.runJob(job_conf1);
            JobClient.runJob(job_conf2);
            JobClient.runJob(job_conf3);
            JobClient.runJob(job_conf4);
            JobClient.runJob(job_conf5);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
