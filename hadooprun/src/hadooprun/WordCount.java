package hadooprun;

import java.io.*;
import java.text.SimpleDateFormat;
import java.util.*;

import org.apache.commons.logging.*;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.bson.*;

import com.mongodb.hadoop.*;
import com.mongodb.hadoop.util.*;

public class WordCount extends MongoTool {

	public static class TokenizerMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

		private final static IntWritable one = new IntWritable(1);
		private Text word = new Text();

		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			// 将输入的纯文本文件的数据转化成String
			String line = value.toString();

			// 将输入的数据首先按行进行分割
			StringTokenizer tokenizerArticle = new StringTokenizer(line, "\n");

			// 分别对每一行进行处理
			while (tokenizerArticle.hasMoreElements()) {
				// 每行按空格划分
				StringTokenizer tokenizerLine = new StringTokenizer(tokenizerArticle.nextToken());

				// String strinfo = tokenizerLine.nextToken().trim();// 日志信息
				// String strnumbers = tokenizerLine.nextToken().trim();//日志数量

				// Text infos = new Text(strinfo);
				// int scoreInt = Integer.parseInt(strnumbers);
				// context.write(infos, new IntWritable(1));

				// String strScore = tokenizerLine.nextToken().trim();// 次数统计部分
				String strName = tokenizerLine.nextToken().trim();//
				String info1 = tokenizerLine.nextToken().trim();// time
				String info = tokenizerLine.nextToken().trim();
				if (tokenizerLine.hasMoreTokens()) {
					String info2 = tokenizerLine.nextToken().trim();// INFO
					if ("INFO".equals(info2)) {
						String info3 = tokenizerLine.nextToken().trim();
						String info4 = tokenizerLine.nextToken().trim();
						if ("登录用户为admin1".equals(info4) || "登录用户为admin2".equals(info4) || "登录用户为admin3".equals(info4) || "登录用户为admin4".equals(info4)
								|| "登录用户为admin5".equals(info4) || "登录用户为admin6".equals(info4) || "登录用户为admin7".equals(info4)
								|| "登录用户为admin8".equals(info4) || "登录用户为admin9".equals(info4) || "登录用户为admin10".equals(info4)
								|| "登录用户为admin11".equals(info4)) {

							if (tokenizerLine.hasMoreTokens()) {
								String info5 = tokenizerLine.nextToken().trim();
								Text infos = new Text(info1 + info4 + info5);
								context.write(infos, new IntWritable(1));
							}

						}
						Text infos = new Text(info1 + info4);
						context.write(infos, new IntWritable(1));
					}

				}

				// String info3 = tokenizerLine.nextToken().trim();//
				// String info4 = tokenizerLine.nextToken().trim();// 日志部分
				// String info5 = tokenizerLine.nextToken().trim();//日志部分
				// int scoreInt = Integer.parseInt(strScore);
				// if("INFO".equals(info2)){
				// String a=info4+info5;
				// Text infos = new Text(a);
				// context.write(infos,new IntWritable(1));
				// }

				// Text infos = new Text(info5);
				// 输出姓名和成绩
				// if("张三".equals(strName)){
				// context.write(name,new IntWritable(1));
				// }

				// context.write(infos,new IntWritable(1));

			}
		}
	}

	public static class IntSumReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

		private final IntWritable result = new IntWritable();

		public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {

			int sum = 0;
			for (final IntWritable val : values) {
				sum += val.get();
			}
			result.set(sum);

			context.write(key, result);
		}

	}

	public static void main(String[] args) throws Exception {

		final Configuration conf = new Configuration();
		// conf.addResource(new Path("F:/lxw-hadoop/hdfs-site.xml"));
		// conf.addResource(new Path("F:/lxw-hadoop/mapred-site.xml"));
		// conf.addResource(new Path("F:/lxw-hadoop/core-site.xml"));
		conf.set("mapred.job.tracker", "localhost:9001");
		MongoConfigUtil.setOutputURI(conf, "mongodb://127.0.0.1:27017/log.osslog");
		System.out.println("Conf: " + conf);

		final Job job = new Job(conf, "word count");

		job.setJarByClass(WordCount.class);

		job.setMapperClass(TokenizerMapper.class);

		job.setReducerClass(IntSumReducer.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);

		job.setOutputFormatClass(MongoOutputFormat.class);
		// MongoConfigUtil.setOutputFormat(conf, MongoOutputFormat.class);
		Date d = new Date();
		SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd");
		FileInputFormat.addInputPath(job, new Path("/" + formatter.format(d)));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
