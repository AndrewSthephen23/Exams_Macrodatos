

import java.io.IOException;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.*;

public class RegressionMapper extends MapReduceBase implements Mapper<LongWritable, Text, Text, Text> {

    public void map(LongWritable key, Text value, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
        if (key.get() == 0) {
            return;
        }
        String valueString = value.toString();
        String[] singleRowData = valueString.split(",");

        if (singleRowData.length >= 11) {
            String frecuencia = singleRowData[7];
            String duracion = singleRowData[8];
            String energia = singleRowData[9];

            try {
                double freq = Double.parseDouble(frecuencia);
                double dur = Double.parseDouble(duracion);
                double energ = Double.parseDouble(energia);

                // Emitir: key="data", value="freq,dur,energia"
                output.collect(new Text("data"), new Text(freq + "," + dur + "," + energ));
            } catch (NumberFormatException e) {
                // Ignorar registros con valores inválidos
            }
        }
    }
}
