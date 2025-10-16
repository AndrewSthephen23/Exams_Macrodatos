

import java.io.IOException;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.*;

public class DurationMapper extends MapReduceBase implements Mapper<LongWritable, Text, Text, Text> {

    public void map(LongWritable key, Text value, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
        if (key.get() == 0) {
            return;
        }
        String valueString = value.toString();
        String[] singleRowData = valueString.split(",");

        if (singleRowData.length >= 11) {
            String datetime = singleRowData[1];
            String tipo = singleRowData[6];
            String duracion = singleRowData[8];

            try {
                // Extraer mes del datetime (formato: 2024-01-01 09:33:37)
                String[] dateParts = datetime.split(" ")[0].split("-");
                String month = dateParts[1];

                double dur = Double.parseDouble(duracion);

                output.collect(new Text(tipo + "," + month), new Text(duracion));
            } catch (Exception e) {
                // Ignorar registros con valores inválidos
            }
        }
    }
}

class DurationMapper2 extends MapReduceBase implements Mapper<LongWritable, Text, Text, Text> {

    public void map(LongWritable key, Text value, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
        String[] rowData = value.toString().split("\t");

        if (rowData.length >= 2) {
            String keyPart = rowData[0];
            String valuePart = rowData[1];

            output.collect(new Text(keyPart), new Text(valuePart));
        }
    }
}

class DurationMapper3 extends MapReduceBase implements Mapper<LongWritable, Text, Text, Text> {

    public void map(LongWritable key, Text value, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
        String[] rowData = value.toString().split("\t");

        if (rowData.length >= 2) {
            String[] keyParts = rowData[0].split(",");
            String valuePart = rowData[1];

            if (keyParts.length >= 2) {
                String tipo = keyParts[0];
                String month = keyParts[1];

                output.collect(new Text(tipo), new Text(month + "," + valuePart));
            }
        }
    }
}
