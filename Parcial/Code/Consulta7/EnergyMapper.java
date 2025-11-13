

import java.io.IOException;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.*;

public class EnergyMapper extends MapReduceBase implements Mapper<LongWritable, Text, Text, Text> {

    public void map(LongWritable key, Text value, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
        if (key.get() == 0) {
            return;
        }
        String valueString = value.toString();
        String[] singleRowData = valueString.split(",");

        if (singleRowData.length >= 11) {
            String datetime = singleRowData[1]; // DATETIME_UTC: 2024-01-01 09:33:37
            String energia = singleRowData[9]; // ENERGIA

            try {
                // Extraer solo la fecha (yyyy-MM-dd)
                String fecha = datetime.split(" ")[0];

                double energy = Double.parseDouble(energia);

                // Emitir (Fecha, Energía)
                output.collect(new Text(fecha), new Text(String.valueOf(energy)));
            } catch (NumberFormatException | ArrayIndexOutOfBoundsException e) {
                // Ignorar registros con valores inválidos
            }
        }
    }
}

class EnergyMapper2 extends MapReduceBase implements Mapper<LongWritable, Text, Text, Text> {

    public void map(LongWritable key, Text value, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
        String[] rowData = value.toString().split("\t");

        if (rowData.length >= 2) {
            String fecha = rowData[0]; // Fecha: 2024-01-01
            String totalEnergia = rowData[1]; // TotalEnergía del día

            try {
                // Extraer el año de la fecha
                String anio = fecha.split("-")[0];

                // Emitir (Año, TotalEnergía)
                output.collect(new Text(anio), new Text(totalEnergia));
            } catch (ArrayIndexOutOfBoundsException e) {
                // Ignorar registros con formato inválido
            }
        }
    }
}

class EnergyMapper3 extends MapReduceBase implements Mapper<LongWritable, Text, Text, Text> {

    public void map(LongWritable key, Text value, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
        String[] rowData = value.toString().split("\t");

        if (rowData.length >= 2) {
            String anio = rowData[0];
            String promedioInfo = rowData[1]; // "PromedioEnergia,NumeroDias"

            // Emitir (Año, AVG:promedio)
            output.collect(new Text(anio), new Text("AVG:" + promedioInfo));
        }
    }
}

class EnergyMapper4 extends MapReduceBase implements Mapper<LongWritable, Text, Text, Text> {

    public void map(LongWritable key, Text value, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
        String[] rowData = value.toString().split("\t");

        if (rowData.length >= 2) {
            String fecha = rowData[0]; // 2024-01-01
            String totalEnergia = rowData[1];

            try {
                String anio = fecha.split("-")[0];

                // Emitir (Año, DAY:fecha:energia)
                output.collect(new Text(anio), new Text("DAY:" + fecha + ":" + totalEnergia));
            } catch (ArrayIndexOutOfBoundsException e) {
                // Ignorar registros inválidos
            }
        }
    }
}

class PassThroughMapper extends MapReduceBase implements Mapper<LongWritable, Text, Text, Text> {

    public void map(LongWritable key, Text value, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
        String[] rowData = value.toString().split("\t");

        if (rowData.length >= 2) {
            String keyPart = rowData[0];
            String valuePart = rowData[1];

            output.collect(new Text(keyPart), new Text(valuePart));
        }
    }
}
