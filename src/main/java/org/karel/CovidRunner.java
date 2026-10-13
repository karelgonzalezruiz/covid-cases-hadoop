package org.karel;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class CovidRunner {

    public static void main(String[] args) throws Exception {

        if (args.length != 2) {
            System.err.println(
                    "Uso: CovidRunner <entrada> <salida>"
            );
            System.exit(1);
        }

        Configuration conf = new Configuration();

        // Utilizar coma como separador del CSV
        conf.set(
                "mapreduce.output.textoutputformat.separator", ","
        );

        Job job = Job.getInstance(conf, "Covid Total Cases");

        job.setJarByClass(CovidRunner.class);

        // Configurar Mapper y Reducer
        job.setMapperClass(CovidMapper.class);
        job.setReducerClass(CovidReducer.class);

        // Tipos de datos del Mapper
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        // Tipos de datos del Reducer
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        // Formato CSV y un unico Reducer
        job.setOutputFormatClass(TextOutputFormat.class);
        job.setNumReduceTasks(1);

        // Rutas de entrada y salida
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        // Ejecutar el trabajo MapReduce
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
