

import java.io.IOException;
import java.util.*;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.*;

public class SeismicReducer extends MapReduceBase implements Reducer<Text, Text, Text, Text> {

    public void reduce(Text t_key, Iterator<Text> values, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
        Text key = t_key;
        double totalEnergy = 0.0;
        int count = 0;

        while (values.hasNext()) {
            Text value = (Text) values.next();
            try {
                double energy = Double.parseDouble(value.toString());
                totalEnergy += energy;
                count++;
            } catch (NumberFormatException e) {
                // Ignorar valores inválidos
            }
        }

        output.collect(key, new Text(totalEnergy + "," + count));
    }
}

class SeismicReducer2 extends MapReduceBase implements Reducer<Text, Text, Text, Text> {

    public void reduce(Text t_key, Iterator<Text> values, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
        Text key = t_key;
        double totalEnergy = 0.0;
        int totalCount = 0;

        while (values.hasNext()) {
            Text value = (Text) values.next();
            String[] parts = value.toString().split(",");

            if (parts.length >= 2) {
                try {
                    double energy = Double.parseDouble(parts[0]);
                    int count = Integer.parseInt(parts[1]);
                    totalEnergy += energy;
                    totalCount += count;
                } catch (NumberFormatException e) {
                    // Ignorar valores inválidos
                }
            }
        }

        output.collect(key, new Text(totalEnergy + "," + totalCount));
    }
}

class SeismicReducer3 extends MapReduceBase implements Reducer<Text, Text, Text, Text> {

    public void reduce(Text t_key, Iterator<Text> values, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
        Text key = t_key;
        double totalEnergy = 0.0;
        int totalCount = 0;

        while (values.hasNext()) {
            Text value = (Text) values.next();
            String[] parts = value.toString().split(",");

            if (parts.length >= 2) {
                try {
                    double energy = Double.parseDouble(parts[0]);
                    int count = Integer.parseInt(parts[1]);
                    totalEnergy += energy;
                    totalCount += count;
                } catch (NumberFormatException e) {
                    // Ignorar valores inválidos
                }
            }
        }

        double average = totalCount > 0 ? totalEnergy / totalCount : 0.0;

        output.collect(key, new Text("Promedio_Energia=" + String.format("%.6f", average) + ",Total_Eventos=" + totalCount));
    }
}
