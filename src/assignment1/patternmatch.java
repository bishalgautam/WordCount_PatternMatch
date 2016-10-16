package assignment1;
import java.lang.*;
import java.io.*;
import java.util.StringTokenizer;
import java.net.URI;
import java.util.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class patternmatch{
    public static class TokenizerMapper
    extends Mapper<Object, Text, Text, IntWritable>{

        private final static IntWritable one = new IntWritable(1);
        private Text word = new Text();
        public HashSet<String> set = new HashSet<String>();
        @Override
        public void setup(Context context) throws IOException{
            URI[] localPaths = null;
            try {
                localPaths = context.getCacheFiles();
            } catch (IOException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }
                FileReader in = new FileReader(new File(localPaths[0].getPath()).getName());
                BufferedReader br = new BufferedReader(in);
                String line; 
                while ((line = br.readLine()) != null) {
                        String[] words = line.split(" ");
                        for(String word : words){
                          set.add(word);
                        }
                }
                in.close();
          //    Scanner scn = new Scanner(new File(localPaths[0].getPath()).getName());            
          //    StringTokenizer nxt = new StringTokenizer(scn.useDelimiter("\\Z").next());
        
        }
        public void map(Object key, Text value, Context context
                        ) throws IOException, InterruptedException {
            StringTokenizer itr = new StringTokenizer(value.toString());
            while (itr.hasMoreTokens()){
                String pattern = itr.nextToken();
                if( set.contains(pattern)){
                 word.set(pattern);
                 context.write(word, one);
                }
            }
        }
    }
    


    public static class IntSumReducer
    extends Reducer<Text,IntWritable,Text,IntWritable> {
        private IntWritable result = new IntWritable();

        public void reduce(Text key, Iterable<IntWritable> values,
                           Context context
                           ) throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
            }
            result.set(sum);
            context.write(key, result);
        }
    }
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "pattern match");
        job.addCacheFile(new Path(args[2]).toUri());
        job.setJarByClass(patternmatch.class);
        job.setMapperClass(TokenizerMapper.class);
        job.setCombinerClass(IntSumReducer.class);
        job.setReducerClass(IntSumReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}

