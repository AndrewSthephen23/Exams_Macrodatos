package estadisticos;

import java.io.IOException;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.*;

public class Consulta3_DesviacionMapper extends MapReduceBase
    implements Mapper<LongWritable, Text, Text, DoubleWritable> {

    // Clave estática para agrupar todos los valores en un único reducer
    private final static Text KEY = new Text("Desviacion Estandar Duracion");

    public void map(LongWritable key, Text value, OutputCollector<Text, DoubleWritable> output, Reporter reporter)
        throws IOException {

        String valueString = value.toString();
        
        // Omitir la cabecera del archivo CSV
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
                // Ignorar filas con datos inválidos
                System.err.println("Error al parsear el valor: " + columns[8]);
            }
        }
    }
}