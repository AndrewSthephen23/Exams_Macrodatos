

import java.io.IOException;
import java.util.*;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.*;

public class DurationReducer extends MapReduceBase implements Reducer<Text, Text, Text, Text> {

    public void reduce(Text t_key, Iterator<Text> values, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
        Text key = t_key;
        double totalDuration = 0.0;
        int count = 0;

        while (values.hasNext()) {
            Text value = (Text) values.next();
            try {
                double duration = Double.parseDouble(value.toString());
                totalDuration += duration;
                count++;
            } catch (NumberFormatException e) {
                // Ignorar valores inválidos
            }
        }

        output.collect(key, new Text(totalDuration + "," + count));
    }
}

class DurationReducer2 extends MapReduceBase implements Reducer<Text, Text, Text, Text> {

    public void reduce(Text t_key, Iterator<Text> values, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
        Text key = t_key;
        double totalDuration = 0.0;
        int totalCount = 0;

        while (values.hasNext()) {
            Text value = (Text) values.next();
            String[] parts = value.toString().split(",");

            if (parts.length >= 2) {
                try {
                    double duration = Double.parseDouble(parts[0]);
                    int count = Integer.parseInt(parts[1]);
                    totalDuration += duration;
                    totalCount += count;
                } catch (NumberFormatException e) {
                    // Ignorar valores inválidos
                }
            }
        }

        double average = totalCount > 0 ? totalDuration / totalCount : 0.0;

        output.collect(key, new Text(String.format("%.2f", totalDuration) + "," + totalCount + "," + String.format("%.2f", average)));
    }
}

class DurationReducer3 extends MapReduceBase implements Reducer<Text, Text, Text, Text> {

    public void reduce(Text t_key, Iterator<Text> values, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
        Text key = t_key;
        double maxAvg = 0.0;
        String peakMonth = "";
        int totalEvents = 0;

        while (values.hasNext()) {
            Text value = (Text) values.next();
            String[] parts = value.toString().split(",");

            if (parts.length >= 4) {
                String month = parts[0];
                double avg = Double.parseDouble(parts[3]);
                int count = Integer.parseInt(parts[2]);
                totalEvents += count;

                if (avg > maxAvg) {
                    maxAvg = avg;
                    peakMonth = month;
                }
            }
        }

        output.collect(key, new Text("Mes_Pico=" + peakMonth + ",Duracion_Promedio_Pico=" + String.format("%.2f", maxAvg) + ",Total_Eventos=" + totalEvents));
    }
}
