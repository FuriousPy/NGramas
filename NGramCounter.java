import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class NGramCounter {

    public static class NGramMapper extends Mapper<Object, Text, Text, IntWritable> {
        private final static IntWritable one = new IntWritable(1);
        private Text word = new Text();

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            // Tokenizar el texto en n-gramas (bi-gramas, tri-gramas, etc.)
            String line = value.toString();
            String[] words = line.split("\\s+");
            int n = context.getConfiguration().getInt("n", 2); // Obtener el valor de N
            if (words.length >= n) {
                for (int i = 0; i <= words.length - n; i++) {
                    StringBuilder ngram = new StringBuilder();
                    for (int j = 0; j < n; j++) {
                        if (j > 0) ngram.append(" ");
                        ngram.append(words[i + j]);
                    }
                    word.set(ngram.toString());
                    context.write(word, one);
                }
            }
        }
    }

    public static class NGramReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
        private IntWritable result = new IntWritable();

        public void reduce(Text key, Iterable<IntWritable> values, Context context)
                throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
            }
            result.set(sum);
            context.write(key, result);
        }
    }

    public static void main(String[] args) throws Exception {
        // Configuración de Hadoop
        Configuration conf = new Configuration();
        conf.setInt("n", Integer.parseInt(args[0])); // Valor de N

        // Eliminar el directorio de salida si existe
        Path outputPath = new Path(args[2]);
        FileSystem fs = outputPath.getFileSystem(conf);
        if (fs.exists(outputPath)) {
            fs.delete(outputPath, true);
        }

        // Configuración del trabajo MapReduce
        Job job = Job.getInstance(conf, "n-gram count");
        job.setJarByClass(NGramCounter.class);
        job.setMapperClass(NGramMapper.class);
        job.setCombinerClass(NGramReducer.class);
        job.setReducerClass(NGramReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        // Directorio de entrada y salida
        FileInputFormat.addInputPath(job, new Path(args[1])); // Directorio de entrada
        FileOutputFormat.setOutputPath(job, outputPath); // Directorio de salida

        // Ejecutar el trabajo MapReduce
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
