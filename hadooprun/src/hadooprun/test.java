package hadooprun;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;  
import org.apache.hadoop.mapreduce.Reducer;
import org.bson.BSONObject; 

import com.mongodb.hadoop.*;
import com.mongodb.hadoop.MongoInputFormat;
import com.mongodb.hadoop.MongoOutputFormat;
import com.mongodb.hadoop.util.MongoConfigUtil;
public class test {  
	  
	// ����һ��Map����   
	   public static class TokenizerMapper extends Mapper<Object, BSONObject, Text, IntWritable> {  
	  
	  
	       private final static IntWritable one = new IntWritable( 1 );  
	       private final Text word = new Text();  
	  
	  
	       public void map( Object key , BSONObject value , Context context ) throws IOException, InterruptedException{  
	  
	  
	           System.out.println( "key: " + key );  
	           System.out.println( "value: " + value );  
	  
	  
	        // �Դʽ��а��ո��з�   
	           final StringTokenizer itr = new StringTokenizer( value.get( "name" ).toString() );  
	           while ( itr.hasMoreTokens() ) {  
	               word.set( itr.nextToken() );  
	               context.write( word, one ); // �����keyΪ�ʣ���valueΪ1   
	           }  
	       }  
	   }  
	  
	  
	// ����Reduce���������ڼ���ʳ��ֵ�Ƶ��   
	   public static class IntSumReducer extends Reducer<Text, IntWritable, Text, IntWritable> {  
	  
	  
	       private final IntWritable result = new IntWritable();  
	  
	  
	       public void reduce( Text key , Iterable<IntWritable> values , Context context ) throws IOException, InterruptedException{  
	  
	        // ����ʳ��ֵ�Ƶ�ʣ�����ͬ�ʵ�value���   
	           int sum = 0;  
	           for ( final IntWritable val : values ) {  
	               sum += val.get();  
	           }  
	           result.set( sum );  
	           context.write( key, result ); // keyΪ������,valueΪ���������Ӧ�Ĵ�Ƶ��   
	       }  
	   }  
	  
	  
	   public static void main( String[] args ) throws Exception{  
	  
	  
	       final Configuration conf = new Configuration();  
	    // ����MongoDB���ݿ����������������������ǵ��ñ��ص�MongoDB��Ĭ�϶˿ں�Ϊ27017   
	       MongoConfigUtil.setInputURI( conf, "mongodb://localhost:27017/data.test");  
	       MongoConfigUtil.setOutputURI( conf, "mongodb://localhost:27017/data.test2");  
	       System.out.println( "Conf: " + conf );  
	  
	  
	       final Job job = new Job( conf , "test" );  
	  
	  
	       job.setJarByClass( test.class );  
	  
	  
	    // ����Mapper,Reduce��Combiner��   
	       job.setMapperClass( TokenizerMapper.class );  
	  
	  
	       job.setCombinerClass( IntSumReducer.class );  
	       job.setReducerClass( IntSumReducer.class );  
	  
	  
	    // ����Mapper��Reduce�����key/value������   
	       job.setOutputKeyClass( Text.class );  
	       job.setOutputValueClass( IntWritable.class );  
	  
	  
	    // ����InputFormat��OutputFormat������   
	       job.setInputFormatClass( MongoInputFormat.class );  
	       job.setOutputFormatClass( MongoOutputFormat.class );  
	  
	  
	       System.exit( job.waitForCompletion( true ) ? 0 : 1 );  
	   } 
}