package estadisticos;

import java.io.IOException;
import java.util.Iterator;
// Importamos los tipos "writable" de Hadoop
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
// Api clasica de Hadoop
import org.apache.hadoop.mapred.*;

public class Consulta1_PromedioReducer extends MapReduceBase
    // Tipos de entrada: Text, DoubleWritable
    // Tipos de salida: Text, DoubleWritable
    implements Reducer<Text, DoubleWritable, Text, DoubleWritable> {

    public void reduce(Text t_key, Iterator<DoubleWritable> values, OutputCollector<Text, DoubleWritable> output, Reporter reporter) 
        throws IOException {
        
        double sum = 0;
        int count = 0;
        
        // Iteramos sobre todos los valores para la clave
        while (values.hasNext()) {
            // Acumulamos la suma y aumentamos el contador
            sum += values.next().get();
            count++;
        }
        
        // Calculamos el promedio
        if (count > 0) {
            double average = sum / count;
            // Emitimos el resultado final
            output.collect(t_key, new DoubleWritable(average));
        }
    }
}