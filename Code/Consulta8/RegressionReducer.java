

import java.io.IOException;
import java.util.*;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.*;

public class RegressionReducer extends MapReduceBase implements Reducer<Text, Text, Text, Text> {

    public void reduce(Text t_key, Iterator<Text> values, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {

        double sumX1 = 0.0, sumX2 = 0.0, sumY = 0.0;
        double sumX1Y = 0.0, sumX2Y = 0.0;
        double sumX1X1 = 0.0, sumX2X2 = 0.0, sumX1X2 = 0.0;
        int n = 0;

        // Recolectar todos los datos
        while (values.hasNext()) {
            Text value = (Text) values.next();
            String[] parts = value.toString().split(",");

            if (parts.length >= 3) {
                try {
                    double x1 = Double.parseDouble(parts[0]); // frecuencia
                    double x2 = Double.parseDouble(parts[1]); // duracion
                    double y = Double.parseDouble(parts[2]);   // energia

                    sumX1 += x1;
                    sumX2 += x2;
                    sumY += y;
                    sumX1Y += x1 * y;
                    sumX2Y += x2 * y;
                    sumX1X1 += x1 * x1;
                    sumX2X2 += x2 * x2;
                    sumX1X2 += x1 * x2;
                    n++;
                } catch (NumberFormatException e) {
                    // Ignorar valores inválidos
                }
            }
        }

        if (n > 2) {
            // Calcular coeficientes de regresión lineal múltiple
            // Modelo: Y = b0 + b1*X1 + b2*X2
            // Usando método de mínimos cuadrados simplificado

            double meanX1 = sumX1 / n;
            double meanX2 = sumX2 / n;
            double meanY = sumY / n;

            double sxx1 = sumX1X1 - (sumX1 * sumX1) / n;
            double sxx2 = sumX2X2 - (sumX2 * sumX2) / n;
            double sxy1 = sumX1Y - (sumX1 * sumY) / n;
            double sxy2 = sumX2Y - (sumX2 * sumY) / n;
            double sx1x2 = sumX1X2 - (sumX1 * sumX2) / n;

            // Coeficientes
            double denominator = (sxx1 * sxx2) - (sx1x2 * sx1x2);

            double b1 = 0.0, b2 = 0.0;
            if (Math.abs(denominator) > 0.0001) {
                b1 = (sxy1 * sxx2 - sxy2 * sx1x2) / denominator;
                b2 = (sxy2 * sxx1 - sxy1 * sx1x2) / denominator;
            }

            double b0 = meanY - b1 * meanX1 - b2 * meanX2;

            // Calcular R²
            double sst = 0.0, sse = 0.0;
            // Necesitaríamos iterar de nuevo sobre los datos para calcular R²
            // Por simplicidad, reportamos los coeficientes

            String result = String.format("Ecuacion: ENERGIA = %.6f + %.6f*FRECUENCIA + %.6f*DURACION, N=%d",
                                         b0, b1, b2, n);

            output.collect(new Text("Modelo_Regresion"), new Text(result));
            output.collect(new Text("Estadisticas"), new Text(
                String.format("Media_Frecuencia=%.2f, Media_Duracion=%.2f, Media_Energia=%.6f",
                             meanX1, meanX2, meanY)));
        } else {
            output.collect(new Text("ERROR"), new Text("Datos insuficientes para regresion"));
        }
    }
}
