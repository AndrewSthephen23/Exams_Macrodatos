package estadisticos;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;

public class Consulta1_PromedioDriver {

    public static void main(String[] args) {
        // Cliente para lanzar el trabajo
        JobClient my_client = new JobClient();
        // Objeto de configuración del trabajo
        JobConf job_conf = new JobConf(Consulta1_PromedioDriver.class);

        // Nombre del trabajo
        job_conf.setJobName("PromedioDuracionSismos");

        // Tipos de datos de la salida final (del Reducer)
        job_conf.setOutputKeyClass(Text.class);
        job_conf.setOutputValueClass(DoubleWritable.class);

        // Tipos de datos de la salida intermedia (del Mapper)
        job_conf.setMapOutputKeyClass(Text.class);
        job_conf.setMapOutputValueClass(DoubleWritable.class);

        // Especificar las clases Mapper y Reducer
        job_conf.setMapperClass(estadisticos.Consulta1_PromedioMapper.class);
        job_conf.setReducerClass(estadisticos.Consulta1_PromedioReducer.class);

        // Formato de los datos de entrada y salida
        job_conf.setInputFormat(TextInputFormat.class);
        job_conf.setOutputFormat(TextOutputFormat.class);

        // Rutas de entrada y salida en HDFS (se pasan como argumentos)
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