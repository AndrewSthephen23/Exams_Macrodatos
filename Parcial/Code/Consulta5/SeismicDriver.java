

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;

public class SeismicDriver {

    public static void main(String[] args) {
        JobClient my_client = new JobClient();

        // Job 1: Extraer tipo, rango de frecuencia y energía
        JobConf job_conf1 = new JobConf(SeismicDriver.class);
        job_conf1.setJobName("SeismicJob1_ExtractData");
        job_conf1.setOutputKeyClass(Text.class);
        job_conf1.setOutputValueClass(Text.class);
        job_conf1.setMapperClass(SeismicMapper.class);
        job_conf1.setReducerClass(SeismicReducer.class);
        job_conf1.setInputFormat(TextInputFormat.class);
        job_conf1.setOutputFormat(TextOutputFormat.class);

        FileInputFormat.setInputPaths(job_conf1, new Path(args[0]));
        FileOutputFormat.setOutputPath(job_conf1, new Path(args[1] + "/temp1"));
        my_client.setConf(job_conf1);

        // Job 2: Agrupar y sumar energías y contar eventos
        JobConf job_conf2 = new JobConf(SeismicDriver.class);
        job_conf2.setJobName("SeismicJob2_SumEnergies");
        job_conf2.setOutputKeyClass(Text.class);
        job_conf2.setOutputValueClass(Text.class);
        job_conf2.setMapperClass(SeismicMapper2.class);
        job_conf2.setReducerClass(SeismicReducer2.class);
        job_conf2.setInputFormat(TextInputFormat.class);
        job_conf2.setOutputFormat(TextOutputFormat.class);

        FileInputFormat.setInputPaths(job_conf2, new Path(args[1] + "/temp1"));
        FileOutputFormat.setOutputPath(job_conf2, new Path(args[1] + "/temp2"));

        // Job 3: Calcular promedio de energía
        JobConf job_conf3 = new JobConf(SeismicDriver.class);
        job_conf3.setJobName("SeismicJob3_CalculateAverage");
        job_conf3.setOutputKeyClass(Text.class);
        job_conf3.setOutputValueClass(Text.class);
        job_conf3.setMapperClass(SeismicMapper3.class);
        job_conf3.setReducerClass(SeismicReducer3.class);
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
