package crecimiento3mr;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;

public class Consulta4_Crecimiento3MRDriver {

    public static void main(String[] args) throws Exception {
        // --- Paths para los directorios ---
        Path inputPath = new Path(args[0]);
        Path finalOutputPath = new Path(args[1]);
        // Directorios temporales para las salidas intermedias
        Path tempOutputPath1 = new Path("temp_crecimiento_1_suma");
        Path tempOutputPath2 = new Path("temp_crecimiento_2_join");

        // --- Job 1: Sumar energía por mes (MR1) ---
        JobConf job1_conf = new JobConf(Consulta4_Crecimiento3MRDriver.class);
        job1_conf.setJobName("MR1_SumaEnergiaPorMes");

        job1_conf.setOutputKeyClass(Text.class);
        job1_conf.setOutputValueClass(DoubleWritable.class);
        
        // Clases del Job 1
        job1_conf.setMapperClass(crecimiento3mr.Consulta4_Mapper.class);
        job1_conf.setReducerClass(crecimiento3mr.Consulta4_Reducer.class);

        job1_conf.setInputFormat(TextInputFormat.class);
        job1_conf.setOutputFormat(TextOutputFormat.class);

        FileInputFormat.setInputPaths(job1_conf, inputPath);
        FileOutputFormat.setOutputPath(job1_conf, tempOutputPath1);

        System.out.println("Lanzando Job 1 (MR1): Agregacion de energia mensual...");
        RunningJob job1 = JobClient.runJob(job1_conf);

        if (!job1.isSuccessful()) {
            System.err.println("Job 1 (MR1) fallo. Abortando.");
            return;
        }
        
        System.out.println("Job 1 (MR1) completado. Lanzando Job 2 (MR2)...");

        // --- Job 2: Autounión para obtener mes anterior (MR2) ---
        JobConf job2_conf = new JobConf(Consulta4_Crecimiento3MRDriver.class);
        job2_conf.setJobName("MR2_JoinMesAnterior");

        job2_conf.setOutputKeyClass(Text.class);
        job2_conf.setOutputValueClass(Text.class);
        
        // Clases del Job 2
        job2_conf.setMapperClass(crecimiento3mr.MR2_Mapper.class);
        job2_conf.setReducerClass(crecimiento3mr.MR2_Reducer.class);

        job2_conf.setInputFormat(TextInputFormat.class);
        job2_conf.setOutputFormat(TextOutputFormat.class);
        
        FileInputFormat.setInputPaths(job2_conf, tempOutputPath1);
        FileOutputFormat.setOutputPath(job2_conf, tempOutputPath2);
        
        RunningJob job2 = JobClient.runJob(job2_conf);

        if (!job2.isSuccessful()) {
            System.err.println("Job 2 (MR2) fallo. Abortando.");
            return;
        }

        System.out.println("Job 2 (MR2) completado. Lanzando Job 3 (MR3)...");

        // --- Job 3: Calcular crecimiento y rankear (MR3) ---
        JobConf job3_conf = new JobConf(Consulta4_Crecimiento3MRDriver.class);
        job3_conf.setJobName("MR3_CalculoYRanking");

        job3_conf.setOutputKeyClass(Text.class);
        job3_conf.setOutputValueClass(Text.class);

        job3_conf.setMapOutputKeyClass(NullWritable.class);
        job3_conf.setMapOutputValueClass(Text.class);
        
        // Clases del Job 3
        job3_conf.setMapperClass(crecimiento3mr.MR3_Mapper.class);
        job3_conf.setReducerClass(crecimiento3mr.MR3_Reducer.class);
        
        job3_conf.setNumReduceTasks(1);

        job3_conf.setInputFormat(TextInputFormat.class);
        job3_conf.setOutputFormat(TextOutputFormat.class);
        
        FileInputFormat.setInputPaths(job3_conf, tempOutputPath2);
        FileOutputFormat.setOutputPath(job3_conf, finalOutputPath);
        
        JobClient.runJob(job3_conf);
        System.out.println("Job 3 (MR3) completado. El resultado final esta en: " + args[1]);
    }
}