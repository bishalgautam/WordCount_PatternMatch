package assignment1;
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


public class wordcount{
        public static class TokenizerMapper
    extends Mapper<Object, Text, Text, IntWritable>{

 private final static IntWritable one = new IntWritable(1);
 private Text word = new Text();
 public HashSet<String> map = new HashSet<String>();
 public void setup(Context context) throws FileNotFoundException{
	 URI[] localPaths = null;
	try {
		localPaths = context.getCacheFiles();
	} catch (IOException e) {
		// TODO Auto-generated catch block
		e.printStackTrace();
	}
	
     StringTokenizer nxt = new StringTokenizer(new Scanner(new File(localPaths[0].getPath())).useDelimiter("\\Z").next());
	
     while(nxt.hasMoreTokens()){
         //String pattern = content.next();
         //if(word == pattern)
        map.add(nxt.nextToken());
        }    
 }
 public void map(Object key, Text value, Context context
                 ) throws IOException, InterruptedException {
   StringTokenizer itr = new StringTokenizer(value.toString());
   while (itr.hasMoreTokens()){
     word.set(itr.nextToken());
       
         
        
        if( map.contains(word)){
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
//JobConf job = new JobConf();
//DistributedCache.addCacheFile(new URI("/user/bible.txt#bible.txt"), 
//                                  job);
job.addCacheFile(new Path(args[2]).toUri());
job.setJarByClass(wordcount.class);
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
