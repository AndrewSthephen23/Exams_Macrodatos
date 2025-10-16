package crecimiento3mr;

import java.io.IOException;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;

// Mapper para el Job 1
public class Consulta4_Mapper extends MapReduceBase implements Mapper<LongWritable, Text, Text, DoubleWritable> {
    public void map(LongWritable key, Text value, OutputCollector<Text, DoubleWritable> output, Reporter reporter) throws IOException {
        String line = value.toString();
        if (line.startsWith("FECHA_CORTE")) return;

        String[] columns = line.split(",");
        if (columns.length > 9) {
            try {
                String datetime = columns[1];
                String yearMonth = datetime.substring(0, 7);
                double energia = Double.parseDouble(columns[9]);
                output.collect(new Text(yearMonth), new DoubleWritable(energia));
            } catch (Exception e) {
                // Ignorar líneas con formato incorrecto
            }
        }
    }
}

// Mapper para el Job 2
class MR2_Mapper extends MapReduceBase implements Mapper<LongWritable, Text, Text, Text> {
    private String getNextMonth(String yyyyMm) {
        String[] parts = yyyyMm.split("-");
        int year = Integer.parseInt(parts[0]);
        int month = Integer.parseInt(parts[1]);
        if (month == 12) {
            return (year + 1) + "-01";
        } else {
            return year + "-" + String.format("%02d", month + 1);
        }
    }

    public void map(LongWritable key, Text value, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
        String line = value.toString();
        String[] parts = line.split("\\s+");
        String month = parts[0];
        String energy = parts[1];

        output.collect(new Text(month), new Text("CURRENT," + energy));
        
        String nextMonth = getNextMonth(month);
        output.collect(new Text(nextMonth), new Text("PREVIOUS," + energy));
    }
}

// Mapper para el Job 3
class MR3_Mapper extends MapReduceBase implements Mapper<LongWritable, Text, NullWritable, Text> {
    public void map(LongWritable key, Text value, OutputCollector<NullWritable, Text> output, Reporter reporter) throws IOException {
        output.collect(NullWritable.get(), value);
    }
}