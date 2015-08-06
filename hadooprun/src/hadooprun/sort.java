package hadooprun;

import java.io.IOException;
import java.util.Iterator;
import java.util.StringTokenizer;
 
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
 
public class sort {
 
    public static class Map extends  Mapper<LongWritable, Text,Text,IntWritable> {
 
        // 实现map函数
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            // 将输入的纯文本文件的数据转化成String
            String line = value.toString();
 
            // 将输入的数据首先按行进行分割
            StringTokenizer tokenizerArticle = new StringTokenizer(line, "\n");
 
            // 分别对每一行进行处理
            while (tokenizerArticle.hasMoreElements()) {
                // 每行按空格划分
                StringTokenizer tokenizerLine = new StringTokenizer(tokenizerArticle.nextToken());
                 
				String strinfo = tokenizerLine.nextToken().trim();// 日志信息
				String strnumbers = tokenizerLine.nextToken().trim();//日志数量
                	 
				Text infos = new Text(strinfo);
				int scoreInt = Integer.parseInt(strnumbers);
				context.write(infos, new IntWritable(scoreInt));

//                String strScore = tokenizerLine.nextToken().trim();// 次数统计部分
//                String strName = tokenizerLine.nextToken().trim();// 
//                String info1 = tokenizerLine.nextToken().trim();// 
//                String info2 = tokenizerLine.nextToken().trim();// 
//                String info3 = tokenizerLine.nextToken().trim();// 
//                String info4 = tokenizerLine.nextToken().trim();//日志部分
                
                //int scoreInt = Integer.parseInt(strScore);
              //  Text infos = new Text(info4);
                // 输出姓名和成绩
//                if("张三".equals(strName)){
//                	 context.write(name,new IntWritable(1));
//                }
                
                
              //  context.write(infos,new IntWritable(1));
               
            }
        }
 
    }
    
    private static IntWritable linenum = new IntWritable(0);
	    public static class Reduce extends Reducer<Text,IntWritable,Text,IntWritable> {
	// 实现reduce函数
	public void reduce(Text key, Iterable<IntWritable> values,
	        Context context) throws IOException, InterruptedException {
	
	    int sum = 0;
	    int count = 0;
	
	    for(IntWritable val:values){
	    	 
            //linenum = new IntWritable(linenum.get()+1);
        }
	    context.write(key,new IntWritable(sum));
	}
	
	}
 
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        // 这句话很关键
        conf.set("mapred.job.tracker", "localhost:9001");
 
        String[] ioArgs = new String[] { "/datain", "/dataout" };
        String[] otherArgs = new GenericOptionsParser(conf, ioArgs).getRemainingArgs();
        if (otherArgs.length != 2) {
            System.err.println("Usage: Score Average <in> <out>");
            System.exit(2);
        }
 
        Job job = new Job(conf, "Score Average");
        job.setJarByClass(check.class);
 
        // 设置Map、Combine和Reduce处理类
        job.setMapperClass(Map.class);
        job.setCombinerClass(Reduce.class);
        job.setReducerClass(Reduce.class);
 
        // 设置输出类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
 
        // 将输入的数据集分割成小数据块splites，提供一个RecordReder的实现
        job.setInputFormatClass(TextInputFormat.class);
        // 提供一个RecordWriter的实现，负责数据输出
        job.setOutputFormatClass(TextOutputFormat.class);
 
        // 设置输入和输出目录
        FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
        FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}