package estadisticos;

import java.io.IOException;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.*;

public class Consulta2_MedianaMapper extends MapReduceBase
    implements Mapper<LongWritable, Text, Text, DoubleWritable> {

    // Clave estática para asegurar que todos los datos vayan a un solo reducer
    private final static Text KEY = new Text("Mediana Duracion");

    public void map(LongWritable key, Text value, OutputCollector<Text, DoubleWritable> output, Reporter reporter)
        throws IOException {

        String valueString = value.toString();
        
        // Ignorar la cabecera del CSV
        if (valueString.startsWith("FECHA_CORTE")) {
            return;
        }

        String[] columns = valueString.split(",");
        
        if (columns.length > 8) {
            try {
                // Extraer el valor de la columna DURACION (índice 8)
                double duracion = Double.parseDouble(columns[8]);
                
                // Emitir el par (clave, valor)
                output.collect(KEY, new DoubleWritable(duracion));
            } catch (NumberFormatException e) {
                // Ignorar filas con datos no numéricos en la columna de duración
                System.err.println("Error al parsear el valor: " + columns[8]);
            }
        }
    }
}