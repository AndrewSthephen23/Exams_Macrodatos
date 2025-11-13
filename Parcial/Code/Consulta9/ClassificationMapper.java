

import java.io.IOException;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.*;

public class ClassificationMapper extends MapReduceBase implements Mapper<LongWritable, Text, Text, Text> {

    public void map(LongWritable key, Text value, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
        if (key.get() == 0) {
            return;
        }
        String valueString = value.toString();
        String[] singleRowData = valueString.split(",");

        if (singleRowData.length >= 11) {
            String tipo = singleRowData[6];
            String frecuencia = singleRowData[7];
            String duracion = singleRowData[8];
            String energia = singleRowData[9];

            try {
                double freq = Double.parseDouble(frecuencia);
                double dur = Double.parseDouble(duracion);
                double energ = Double.parseDouble(energia);

                // Emitir tipo con features para calcular estadísticas
                output.collect(new Text(tipo), new Text(freq + "," + dur + "," + energ));
            } catch (NumberFormatException e) {
                // Ignorar registros con valores inválidos
            }
        }
    }
}
