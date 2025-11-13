

import java.io.IOException;
import java.util.*;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.*;

public class ClassificationReducer extends MapReduceBase implements Reducer<Text, Text, Text, Text> {

    public void reduce(Text t_key, Iterator<Text> values, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
        Text key = t_key;

        double sumFreq = 0.0, sumDur = 0.0, sumEnerg = 0.0;
        double sumFreqSq = 0.0, sumDurSq = 0.0, sumEnergSq = 0.0;
        int count = 0;

        // Calcular estadísticas por tipo
        while (values.hasNext()) {
            Text value = (Text) values.next();
            String[] parts = value.toString().split(",");

            if (parts.length >= 3) {
                try {
                    double freq = Double.parseDouble(parts[0]);
                    double dur = Double.parseDouble(parts[1]);
                    double energ = Double.parseDouble(parts[2]);

                    sumFreq += freq;
                    sumDur += dur;
                    sumEnerg += energ;

                    sumFreqSq += freq * freq;
                    sumDurSq += dur * dur;
                    sumEnergSq += energ * energ;

                    count++;
                } catch (NumberFormatException e) {
                    // Ignorar valores inválidos
                }
            }
        }

        if (count > 0) {
            // Calcular media y desviación estándar
            double meanFreq = sumFreq / count;
            double meanDur = sumDur / count;
            double meanEnerg = sumEnerg / count;

            double varFreq = (sumFreqSq / count) - (meanFreq * meanFreq);
            double varDur = (sumDurSq / count) - (meanDur * meanDur);
            double varEnerg = (sumEnergSq / count) - (meanEnerg * meanEnerg);

            double stdFreq = Math.sqrt(Math.max(0, varFreq));
            double stdDur = Math.sqrt(Math.max(0, varDur));
            double stdEnerg = Math.sqrt(Math.max(0, varEnerg));

            // Probabilidad a priori
            double prior = (double) count;

            output.collect(key, new Text(
                String.format("N=%d,P(clase)=%.4f,Media_Freq=%.2f,Std_Freq=%.2f,Media_Dur=%.2f,Std_Dur=%.2f,Media_Energ=%.6f,Std_Energ=%.6f",
                             count, prior, meanFreq, stdFreq, meanDur, stdDur, meanEnerg, stdEnerg)));

            // Emitir reglas de clasificación basadas en umbrales
            output.collect(new Text(key + "_Reglas"), new Text(
                String.format("Si Frecuencia cerca de %.2f y Duracion cerca de %.2f entonces probablemente es %s",
                             meanFreq, meanDur, key.toString())));
        }
    }
}
