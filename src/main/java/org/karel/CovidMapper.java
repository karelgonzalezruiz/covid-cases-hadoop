package org.karel;
import java.io.IOException;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class CovidMapper extends Mapper<LongWritable, Text, Text, Text> {

    private final Text country = new Text();
    private final Text dateAndCases = new Text();

    @Override
    protected void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {

        String line = value.toString();

        // Ignorar encabezado y lineas vacias
        if (line.startsWith("iso_code,") || line.trim().isEmpty()) {
            return;
        }

        String[] fields = line.split(",", -1);

        if (fields.length < 5) {
            return;
        }

        String continent = fields[1].trim();
        String location = fields[2].trim();
        String date = fields[3].trim();
        String totalCases = fields[4].trim();

        // Excluir regiones, agrupaciones y datos incompletos
        if (continent.isEmpty() || location.isEmpty()
                || date.isEmpty() || totalCases.isEmpty()) {
            return;
        }

        try {
            double numericCases = Double.parseDouble(totalCases);

            if (!Double.isFinite(numericCases) || numericCases < 0) {
                return;
            }

            country.set(location);
            dateAndCases.set(date + "|" + Math.round(numericCases));

            context.write(country, dateAndCases);

        } catch (NumberFormatException e) {
            // Ignorar valores numericos invalidos
        }
    }
}
