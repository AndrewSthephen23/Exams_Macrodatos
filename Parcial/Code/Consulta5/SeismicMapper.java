

import java.io.IOException;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.*;

public class SeismicMapper extends MapReduceBase implements Mapper<LongWritable, Text, Text, Text> {

    public void map(LongWritable key, Text value, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
        if (key.get() == 0) {
            return;
        }
        String valueString = value.toString();
        String[] singleRowData = valueString.split(",");

        if (singleRowData.length >= 11) {
            String tipo = singleRowData[6];
            String frecuencia = singleRowData[7];
            String energia = singleRowData[9];

            try {
                double freq = Double.parseDouble(frecuencia);
                String freqRange;

                if (freq < 3.0) {
                    freqRange = "Baja";
                } else if (freq < 6.0) {
                    freqRange = "Media";
                } else {
                    freqRange = "Alta";
                }

                output.collect(new Text(tipo + "," + freqRange), new Text(energia));
            } catch (NumberFormatException e) {
                // Ignorar registros con valores inválidos
            }
        }
    }
}

class SeismicMapper2 extends MapReduceBase implements Mapper<LongWritable, Text, Text, Text> {

    public void map(LongWritable key, Text value, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
        String[] rowData = value.toString().split("\t");

        if (rowData.length >= 2) {
            String keyPart = rowData[0];
            String valuePart = rowData[1];

            output.collect(new Text(keyPart), new Text(valuePart));
        }
    }
}

class SeismicMapper3 extends MapReduceBase implements Mapper<LongWritable, Text, Text, Text> {

    public void map(LongWritable key, Text value, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
        String[] rowData = value.toString().split("\t");

        if (rowData.length >= 2) {
            String keyPart = rowData[0];
            String valuePart = rowData[1];

            output.collect(new Text(keyPart), new Text(valuePart));
        }
    }
}
