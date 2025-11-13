package estadisticos;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.*;

public class Consulta2_MedianaReducer extends MapReduceBase
    implements Reducer<Text, DoubleWritable, Text, DoubleWritable> {

    public void reduce(Text t_key, Iterator<DoubleWritable> values, OutputCollector<Text, DoubleWritable> output, Reporter reporter) 
        throws IOException {
        
        // 1. Almacenar todos los valores en una lista
        ArrayList<Double> listaDuraciones = new ArrayList<>();
        while (values.hasNext()) {
            listaDuraciones.add(values.next().get());
        }
        
        // Si la lista no está vacía, proceder a calcular
        if (!listaDuraciones.isEmpty()) {
            // 2. Ordenar la lista
            Collections.sort(listaDuraciones);
            
            int n = listaDuraciones.size();
            double mediana;
            
            // 3. Calcular la mediana
            if (n % 2 == 1) {
                // Si el número de elementos es impar, la mediana es el valor del medio
                mediana = listaDuraciones.get(n / 2);
            } else {
                // Si es par, es el promedio de los dos valores centrales
                double medio1 = listaDuraciones.get(n / 2 - 1);
                double medio2 = listaDuraciones.get(n / 2);
                mediana = (medio1 + medio2) / 2.0;
            }
            
            // 4. Emitir el resultado
            output.collect(t_key, new DoubleWritable(mediana));
        }
    }
}