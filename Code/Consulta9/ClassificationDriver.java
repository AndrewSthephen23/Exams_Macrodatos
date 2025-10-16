

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;

public class ClassificationDriver {

    public static void main(String[] args) {
        JobClient my_client = new JobClient();

        // Job único: Calcular parámetros para clasificación Naive Bayes
        JobConf job_conf = new JobConf(ClassificationDriver.class);
        job_conf.setJobName("ClassificationJob_NaiveBayes");
        job_conf.setOutputKeyClass(Text.class);
        job_conf.setOutputValueClass(Text.class);
        job_conf.setMapperClass(ClassificationMapper.class);
        job_conf.setReducerClass(ClassificationReducer.class);
        job_conf.setInputFormat(TextInputFormat.class);
        job_conf.setOutputFormat(TextOutputFormat.class);

        FileInputFormat.setInputPaths(job_conf, new Path(args[0]));
        FileOutputFormat.setOutputPath(job_conf, new Path(args[1]));
        my_client.setConf(job_conf);

        try {
            JobClient.runJob(job_conf);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
