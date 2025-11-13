package estadisticos;

import java.io.IOException;
// Importamos los tipos "writable" de Hadoop
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
// Api clasica de Hadoop
import org.apache.hadoop.mapred.*;

public class Consulta1_PromedioMapper extends MapReduceBase
    // Tipos de entrada: LongWritable (offset), Text (línea)
    // Tipos de salida: Text (clave), DoubleWritable (valor)
    implements Mapper<LongWritable, Text, Text, DoubleWritable> {

    // Clave de salida única para agrupar todos los valores
    private final static Text KEY = new Text("Promedio Duracion");

    public void map(LongWritable key, Text value, OutputCollector<Text, DoubleWritable> output, Reporter reporter)
        throws IOException {

        String valueString = value.toString();
        
        // Ignoramos la cabecera del CSV
        if (valueString.startsWith("FECHA_CORTE")) {
            return;
        }

        // Dividimos la línea por comas
        String[] columns = valueString.split(",");
        
        // Verificamos que la fila tenga la cantidad esperada de columnas
        if (columns.length > 8) {
            try {
                // Extraemos el valor de la columna DURACION (índice 8)
                double duracion = Double.parseDouble(columns[8]);
                
                // Emitimos el par (clave, valor)
                output.collect(KEY, new DoubleWritable(duracion));
            } catch (NumberFormatException e) {
                // Ignoramos las filas donde la duración no sea un número válido
                System.err.println("Error al parsear el valor: " + columns[8]);
            }
        }
    }
}