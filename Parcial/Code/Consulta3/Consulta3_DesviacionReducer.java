package estadisticos;

import java.io.IOException;
import java.util.Iterator;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.*;

public class Consulta3_DesviacionReducer extends MapReduceBase
    implements Reducer<Text, DoubleWritable, Text, DoubleWritable> {

    public void reduce(Text t_key, Iterator<DoubleWritable> values, OutputCollector<Text, DoubleWritable> output, Reporter reporter) 
        throws IOException {
        
        long count = 0;
        double sum = 0.0;
        double sumOfSquares = 0.0;
        
        // 1. Iterar sobre los valores para calcular los agregados necesarios
        while (values.hasNext()) {
            double currentValue = values.next().get();
            count++;
            sum += currentValue;
            sumOfSquares += currentValue * currentValue;
        }
        
        // 2. Calcular la desviación estándar si hay datos
        if (count > 0) {
            // Calcular la media (promedio)
            double mean = sum / count;
            
            // Calcular la varianza usando la fórmula de un solo paso
            // Varianza = (Σx² / N) - μ²
            double variance = (sumOfSquares / count) - (mean * mean);
            
            // La desviación estándar es la raíz cuadrada de la varianza
            double stdDev = Math.sqrt(variance);
            
            // 3. Emitir el resultado final
            output.collect(t_key, new DoubleWritable(stdDev));
        }
    }
}