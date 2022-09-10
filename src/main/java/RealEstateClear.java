import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class RealEstateClear {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        //获取hdfs配置对象
        Configuration conf = new Configuration();
        //设置工作对象
        Job job = new Job(conf,"RealEstateClear");
        job.setJarByClass(RealEstateClear.class);
        //设置Mapper执行类
        job.setMapperClass(ClearMapper.class);
        job.setMapOutputKeyClass(NullWritable.class);
        job.setMapOutputValueClass(Text.class);

        //设置不需要reduce类参与执行
        job.setNumReduceTasks(0);
        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(Text.class);

        //设置输入类
        job.setInputFormatClass(TextInputFormat.class);
        //输入类读取hdfs上的数据
        FileInputFormat.addInputPath(job,new Path("/UrbanEconomy/gather/3/"));
        //输出类写入hdfs数据到指定路径
        Path outPutPath = new Path("/UrbanEconomy/dataclear/estate/");
        FileOutputFormat.setOutputPath(job,outPutPath);
        //判断是否存在输出路径，如果存在自动删除
        FileSystem fs = FileSystem.get(conf);
        if (fs.exists(outPutPath)) {
            fs.delete(outPutPath, true);
        }
        //提交此次工作对象的执行命令并判断是否成功，如不成功程序推出
        System.exit(job.waitForCompletion(true)?0:1);
    }
    public static class ClearMapper extends Mapper<LongWritable,Text,NullWritable,Text>{
        public void map(LongWritable key,Text value,Context context) throws IOException, InterruptedException {
            String[] str = value.toString().split(",");
            //判断读取的该行数据的格式是否正确（拆分的数组长度为9，省份和总投资额不能为空）
            if (str.length==9 && !"".equals(str[1]) && !"".equals(str[4])){
                //写入数据到hdfs文件中，过滤掉无用字段
                context.write(NullWritable.get(),new Text(str[1]+","+str[4]));
            }
        }
    }
}
