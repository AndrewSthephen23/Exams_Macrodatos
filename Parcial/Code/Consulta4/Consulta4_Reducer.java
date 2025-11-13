package crecimiento3mr;

import java.io.IOException;
import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.AbstractMap.SimpleEntry;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;

// Reducer para el Job 1
public class Consulta4_Reducer extends MapReduceBase implements Reducer<Text, DoubleWritable, Text, DoubleWritable> {
    public void reduce(Text key, Iterator<DoubleWritable> values, OutputCollector<Text, DoubleWritable> output, Reporter reporter) throws IOException {
        double sum = 0;
        while (values.hasNext()) {
            sum += values.next().get();
        }
        output.collect(key, new DoubleWritable(sum));
    }
}

// Reducer para el Job 2
class MR2_Reducer extends MapReduceBase implements Reducer<Text, Text, Text, Text> {
    public void reduce(Text key, Iterator<Text> values, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
        double currentEnergy = -1;
        double previousEnergy = -1;

        while (values.hasNext()) {
            String line = values.next().toString();
            String[] parts = line.split(",");
            if (parts[0].equals("CURRENT")) {
                currentEnergy = Double.parseDouble(parts[1]);
            } else if (parts[0].equals("PREVIOUS")) {
                previousEnergy = Double.parseDouble(parts[1]);
            }
        }

        if (currentEnergy != -1 && previousEnergy != -1) {
            output.collect(key, new Text(currentEnergy + "," + previousEnergy));
        }
    }
}

// Reducer para el Job 3
class MR3_Reducer extends MapReduceBase implements Reducer<NullWritable, Text, Text, Text> {
    private static final DecimalFormat df = new DecimalFormat("0.00");

    public void reduce(NullWritable key, Iterator<Text> values, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
        List<SimpleEntry<String, Double>> results = new ArrayList<>();

        while (values.hasNext()) {
            String line = values.next().toString();
            String[] monthAndEnergies = line.split("\\s+");
            String month = monthAndEnergies[0];
            String[] energies = monthAndEnergies[1].split(",");
            
            double currentEnergy = Double.parseDouble(energies[0]);
            double previousEnergy = Double.parseDouble(energies[1]);

            double growthRate = (previousEnergy > 0) ? ((currentEnergy - previousEnergy) / previousEnergy) * 100 : 0;
            results.add(new SimpleEntry<>(month, growthRate));
        }

        Collections.sort(results, new Comparator<SimpleEntry<String, Double>>() {
            @Override
            public int compare(SimpleEntry<String, Double> o1, SimpleEntry<String, Double> o2) {
                return o2.getValue().compareTo(o1.getValue());
            }
        });

        int rank = 1;
        for (SimpleEntry<String, Double> entry : results) {
            String month = entry.getKey();
            String growth = df.format(entry.getValue()) + "%";
            String finalValue = "Rank: " + rank + ", Crecimiento: " + growth;
            output.collect(new Text(month), new Text(finalValue));
            rank++;
        }
    }
}