

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;

public class RegressionDriver {

    public static void main(String[] args) {
        JobClient my_client = new JobClient();

        // Job único: Calcular regresión lineal múltiple
        JobConf job_conf = new JobConf(RegressionDriver.class);
        job_conf.setJobName("RegressionJob_LinearModel");
        job_conf.setOutputKeyClass(Text.class);
        job_conf.setOutputValueClass(Text.class);
        job_conf.setMapperClass(RegressionMapper.class);
        job_conf.setReducerClass(RegressionReducer.class);
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
