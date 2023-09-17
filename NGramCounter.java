import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
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

    public static class NGramMapper extends Mapper<Object, Text, Text, Text> {
        private static final Text outKey = new Text();
        private static final Text outValue = new Text();
        private static final String DELIMITER = ",";

        private Map<String, String> fileMap;

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            fileMap = new HashMap<>();
        }

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            // Tokenizar el texto en n-gramas (bi-gramas, tri-gramas, etc.)
            String line = value.toString();
            String[] words = line.split("\\s+");
            int n = context.getConfiguration().getInt("n", 2); // Obtener el valor de N
            String fileName = ((org.apache.hadoop.mapreduce.lib.input.FileSplit) context.getInputSplit()).getPath().getName();

            if (words.length >= n) {
                for (int i = 0; i <= words.length - n; i++) {
                    StringBuilder ngram = new StringBuilder();
                    for (int j = 0; j < n; j++) {
                        if (j > 0) ngram.append(" ");
                        ngram.append(words[i + j]);
                    }
                    outKey.set(ngram.toString());
                    String fileValue = fileMap.getOrDefault(outKey.toString(), "");
                    if (!fileValue.isEmpty()) {
                        fileValue += DELIMITER;
                    }
                    fileValue += fileName;
                    fileMap.put(outKey.toString(), fileValue);
                }
            }
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            for (Map.Entry<String, String> entry : fileMap.entrySet()) {
                outKey.set(entry.getKey());
                outValue.set(entry.getValue());
                context.write(outKey, outValue);
            }
        }
    }

    public static class NGramReducer extends Reducer<Text, Text, Text, Text> {
        private Text result = new Text();
        private static final String DELIMITER = ",";

        private int minFrequency;

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            minFrequency = context.getConfiguration().getInt("minFrequency", 1);
        }

        public void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {
            int sum = 0;
            List<String> files = new ArrayList<>();

            for (Text val : values) {
                String[] parts = val.toString().split(DELIMITER);
                sum += parts.length;
                for (String file : parts) {
                    if (!files.contains(file)) {
                        files.add(file);
                    }
                }
            }

            if (sum >= minFrequency) {
                StringBuilder resultStr = new StringBuilder();
                resultStr.append(sum).append("\t");
                resultStr.append(String.join(", ", files));
                result.set(resultStr.toString());
                context.write(key, result);
            }
        }
    }

    public static void main(String[] args) throws Exception {
        if (args.length != 4) {
            System.err.println("Uso: NGramCounter <n> <minFrequency> <inputPath> <outputPath>");
            System.exit(2);
        }

        // Configuración de Hadoop
        Configuration conf = new Configuration();
        conf.setInt("n", Integer.parseInt(args[0])); // Valor de N
        conf.setInt("minFrequency", Integer.parseInt(args[1])); // Número mínimo de n-gramas

        // Eliminar el directorio de salida si existe
        Path outputPath = new Path(args[3]);
        FileSystem fs = outputPath.getFileSystem(conf);
        if (fs.exists(outputPath)) {
            fs.delete(outputPath, true);
        }

        // Configuración del trabajo MapReduce
        Job job = Job.getInstance(conf, "n-gram count");
        job.setJarByClass(NGramCounter.class);
        job.setMapperClass(NGramMapper.class);
        job.setReducerClass(NGramReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        // Directorio de entrada y salida
        FileInputFormat.addInputPath(job, new Path(args[2])); // Directorio de entrada
        FileOutputFormat.setOutputPath(job, outputPath); // Directorio de salida

        // Ejecutar el trabajo MapReduce
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}