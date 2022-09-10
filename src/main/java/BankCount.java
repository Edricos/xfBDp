import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.*;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.HashMap;


public class BankCount {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException, URISyntaxException {
        //获取hdfs配置对象
        Configuration conf = new Configuration();
        //设置工作对象
        Job job = new Job(conf,"bankcount");
        job.setJarByClass(BankCount.class);
        //设置Mapper执行类
        job.setMapperClass(BankMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(LongWritable.class);
        //设置reduce执行类
        job.setReducerClass(BankReducer.class);
        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(Text.class);
        //设置输入类
        job.setInputFormatClass(TextInputFormat.class);
        //输入类读取hdfs上的数据
        FileInputFormat.addInputPath(job,new Path("/UrbanEconomy/dataclear/bank/"));
        //输出类写入hdfs数据到指定路径
        Path outPutPath = new Path("/UrbanEconomy/count/bank/");
        FileOutputFormat.setOutputPath(job,outPutPath);
        //判断是否存在输出路径，如果存在自动删除
        FileSystem fs = FileSystem.get(conf);
        if (fs.exists(outPutPath)) {
            fs.delete(outPutPath, true);
        }
        //提交此次工作对象的执行命令并判断是否成功，如不成功程序推出
        System.exit(job.waitForCompletion(true)?0:1);
    }

    public static class BankMapper extends Mapper<LongWritable,Text,Text,LongWritable>{

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            //切分读取到改行字符串封装数组
            String[] split = value.toString().split(",");
            context.write(new Text(split[0]),new LongWritable(Long.parseLong(split[1])));
        }
    }

    public static class BankReducer extends Reducer<Text,LongWritable,NullWritable,Text>{
        @Override
        protected void reduce(Text key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {
            //设置累加变量，统计省份对应总贷款额
            long count = 0;
            //遍历省份对应贷款金额
            for (LongWritable value:
                 values) {
                //累加贷款金额
                count += value.get();
            }
            //写入输出文件
            context.write(NullWritable.get(), new Text(key.toString()+","+count));

        }
    }

}
