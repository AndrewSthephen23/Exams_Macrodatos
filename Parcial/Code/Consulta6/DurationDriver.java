

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;

public class DurationDriver {

    public static void main(String[] args) {
        JobClient my_client = new JobClient();

        // Job 1: Extraer tipo, mes y duración
        JobConf job_conf1 = new JobConf(DurationDriver.class);
        job_conf1.setJobName("DurationJob1_ExtractData");
        job_conf1.setOutputKeyClass(Text.class);
        job_conf1.setOutputValueClass(Text.class);
        job_conf1.setMapperClass(DurationMapper.class);
        job_conf1.setReducerClass(DurationReducer.class);
        job_conf1.setInputFormat(TextInputFormat.class);
        job_conf1.setOutputFormat(TextOutputFormat.class);

        FileInputFormat.setInputPaths(job_conf1, new Path(args[0]));
        FileOutputFormat.setOutputPath(job_conf1, new Path(args[1] + "/temp1"));
        my_client.setConf(job_conf1);

        // Job 2: Calcular promedios por tipo y mes
        JobConf job_conf2 = new JobConf(DurationDriver.class);
        job_conf2.setJobName("DurationJob2_CalculateAverages");
        job_conf2.setOutputKeyClass(Text.class);
        job_conf2.setOutputValueClass(Text.class);
        job_conf2.setMapperClass(DurationMapper2.class);
        job_conf2.setReducerClass(DurationReducer2.class);
        job_conf2.setInputFormat(TextInputFormat.class);
        job_conf2.setOutputFormat(TextOutputFormat.class);

        FileInputFormat.setInputPaths(job_conf2, new Path(args[1] + "/temp1"));
        FileOutputFormat.setOutputPath(job_conf2, new Path(args[1] + "/temp2"));

        // Job 3: Encontrar mes pico y estadísticas por tipo
        JobConf job_conf3 = new JobConf(DurationDriver.class);
        job_conf3.setJobName("DurationJob3_FindPeakMonth");
        job_conf3.setOutputKeyClass(Text.class);
        job_conf3.setOutputValueClass(Text.class);
        job_conf3.setMapperClass(DurationMapper3.class);
        job_conf3.setReducerClass(DurationReducer3.class);
        job_conf3.setInputFormat(TextInputFormat.class);
        job_conf3.setOutputFormat(TextOutputFormat.class);

        FileInputFormat.setInputPaths(job_conf3, new Path(args[1] + "/temp2"));
        FileOutputFormat.setOutputPath(job_conf3, new Path(args[1] + "/final"));

        try {
            JobClient.runJob(job_conf1);
            JobClient.runJob(job_conf2);
            JobClient.runJob(job_conf3);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
