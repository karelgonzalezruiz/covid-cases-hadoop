package org.karel;
import java.io.IOException;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class CovidReducer extends Reducer<Text, Text, Text, Text> {

    private final Text result = new Text();

    @Override
    protected void setup(Context context)
            throws IOException, InterruptedException {

        // Encabezado del CSV
        context.write(
                new Text("pais"),
                new Text("total_casos")
        );
    }

    @Override
    protected void reduce(Text country, Iterable<Text> values,
                          Context context)
            throws IOException, InterruptedException {

        String latestDate = "";
        String latestCases = "";

        // Buscar el registro mas reciente de cada pais
        for (Text value : values) {

            String[] parts = value.toString().split("\\|", 2);

            if (parts.length != 2) {
                continue;
            }

            String date = parts[0];
            String cases = parts[1];

            if (date.compareTo(latestDate) > 0) {
                latestDate = date;
                latestCases = cases;
            }
        }

        // Escribir el total final del pais
        if (!latestDate.isEmpty()) {
            result.set(latestCases);
            context.write(country, result);
        }
    }
}
