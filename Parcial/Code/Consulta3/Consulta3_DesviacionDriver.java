package estadisticos;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;

public class Consulta3_DesviacionDriver {

    public static void main(String[] args) {
        JobClient my_client = new JobClient();
        JobConf job_conf = new JobConf(Consulta3_DesviacionDriver.class);

        // Nombre descriptivo para el trabajo
        job_conf.setJobName("DesviacionEstandarDuracionSismos");

        // Tipos de datos de la salida final
        job_conf.setOutputKeyClass(Text.class);
        job_conf.setOutputValueClass(DoubleWritable.class);

        // Tipos de datos de la salida del Mapper
        job_conf.setMapOutputKeyClass(Text.class);
        job_conf.setMapOutputValueClass(DoubleWritable.class);

        // Especificar las clases Mapper y Reducer a utilizar
        job_conf.setMapperClass(estadisticos.Consulta3_DesviacionMapper.class);
        job_conf.setReducerClass(estadisticos.Consulta3_DesviacionReducer.class);

        // Formato de los datos de entrada y salida
        job_conf.setInputFormat(TextInputFormat.class);
        job_conf.setOutputFormat(TextOutputFormat.class);

        // Rutas de entrada y salida en HDFS
        FileInputFormat.setInputPaths(job_conf, new Path(args[0]));
        FileOutputFormat.setOutputPath(job_conf, new Path(args[1]));

        my_client.setConf(job_conf);
        try {
            // Lanzar el trabajo y esperar a que termine
            JobClient.runJob(job_conf);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}