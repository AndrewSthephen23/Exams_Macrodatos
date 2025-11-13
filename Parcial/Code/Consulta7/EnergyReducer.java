

import java.io.IOException;
import java.util.*;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.*;

public class EnergyReducer extends MapReduceBase implements Reducer<Text, Text, Text, Text> {

    public void reduce(Text t_key, Iterator<Text> values, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
        Text key = t_key;
        double totalEnergy = 0.0;

        // MR1: Sumar toda la energía del día
        while (values.hasNext()) {
            Text value = (Text) values.next();
            try {
                double energy = Double.parseDouble(value.toString());
                totalEnergy += energy;
            } catch (NumberFormatException e) {
                // Ignorar valores inválidos
            }
        }

        // Emitir (Fecha, EnergíaTotalDelDía)
        output.collect(key, new Text(String.valueOf(totalEnergy)));
    }
}

class EnergyReducer2 extends MapReduceBase implements Reducer<Text, Text, Text, Text> {

    public void reduce(Text t_key, Iterator<Text> values, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
        Text key = t_key;
        double sumEnergy = 0.0;
        int dayCount = 0;

        // MR2: Calcular el promedio de energía diaria del año
        while (values.hasNext()) {
            Text value = (Text) values.next();
            try {
                double energy = Double.parseDouble(value.toString());
                sumEnergy += energy;
                dayCount++;
            } catch (NumberFormatException e) {
                // Ignorar valores inválidos
            }
        }

        double averageEnergy = dayCount > 0 ? sumEnergy / dayCount : 0.0;

        // Emitir (Año, "PromedioEnergía,NúmeroDeDías")
        output.collect(key, new Text(averageEnergy + "," + dayCount));
    }
}

class EnergyReducer3 extends MapReduceBase implements Reducer<Text, Text, Text, Text> {

    public void reduce(Text t_key, Iterator<Text> values, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
        Text key = t_key;

        double avgEnergy = 0.0;
        List<String> days = new ArrayList<String>();

        // MR3: Clasificar días y contar los de "Alta Energía"
        while (values.hasNext()) {
            Text value = (Text) values.next();
            String valueStr = value.toString();

            if (valueStr.startsWith("AVG:")) {
                // Información del promedio
                String avgInfo = valueStr.substring(4); // Remover "AVG:"
                String[] parts = avgInfo.split(",");
                if (parts.length >= 1) {
                    try {
                        avgEnergy = Double.parseDouble(parts[0]);
                    } catch (NumberFormatException e) {
                        // Ignorar
                    }
                }
            } else if (valueStr.startsWith("DAY:")) {
                // Información de un día específico
                days.add(valueStr.substring(4)); // Remover "DAY:"
            }
        }

        // Umbral de Alta Energía: Promedio * 1.5
        double threshold = avgEnergy * 1.5;

        int altaEnergiaCount = 0;
        int mediaEnergiaCount = 0;
        int bajaEnergiaCount = 0;

        for (String dayInfo : days) {
            String[] parts = dayInfo.split(":");
            if (parts.length >= 2) {
                try {
                    String fecha = parts[0];
                    double dayEnergy = Double.parseDouble(parts[1]);

                    if (dayEnergy >= threshold) {
                        altaEnergiaCount++;
                    } else if (dayEnergy >= avgEnergy * 0.5) {
                        mediaEnergiaCount++;
                    } else {
                        bajaEnergiaCount++;
                    }
                } catch (NumberFormatException e) {
                    // Ignorar
                }
            }
        }

        // Emitir resultados
        output.collect(new Text("Alta_Energia"), new Text(String.valueOf(altaEnergiaCount)));
        output.collect(new Text("Media_Energia"), new Text(String.valueOf(mediaEnergiaCount)));
        output.collect(new Text("Baja_Energia"), new Text(String.valueOf(bajaEnergiaCount)));
        output.collect(new Text("Promedio_Diario_Anual"), new Text(String.format("%.6f", avgEnergy)));
        output.collect(new Text("Umbral_Alta_Energia"), new Text(String.format("%.6f", threshold)));
    }
}

class PassThroughReducer extends MapReduceBase implements Reducer<Text, Text, Text, Text> {

    public void reduce(Text t_key, Iterator<Text> values, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
        Text key = t_key;

        while (values.hasNext()) {
            Text value = (Text) values.next();
            output.collect(key, value);
        }
    }
}
