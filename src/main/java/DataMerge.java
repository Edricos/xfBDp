import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.HashMap;

public class DataMerge {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException, URISyntaxException {
        //获取hdfs配置对象
        Configuration conf = new Configuration();
        //设置工作对象
        Job job = new Job(conf,"merge");
        job.setJarByClass(DataMerge.class);

        //加入2017年各省地方财政收入信息hdfs缓存文件
        job.addCacheFile(new URI("/UrbanEconomy/gather/1/2017_local_financial_revenue.csv"));

        //设置Mapper执行类
        job.setMapperClass(MergeMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        //设置reduce执行类
        job.setReducerClass(MergeReducer.class);
        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(Text.class);
        //设置输入类
        job.setInputFormatClass(TextInputFormat.class);
        //输入类读取hdfs上的数据（*号作用是读取count目录下所有的文件夹下的文件）
        FileInputFormat.addInputPath(job,new Path("/UrbanEconomy/count/*/"));
        //输出类写入hdfs数据到指定路径
        Path outPutPath = new Path("/UrbanEconomy/merge/");
        FileOutputFormat.setOutputPath(job,outPutPath);
        //判断是否存在输出路径，如果存在自动删除
        FileSystem fs = FileSystem.get(conf);
        if (fs.exists(outPutPath)) {
            fs.delete(outPutPath, true);
        }
        //提交此次工作对象的执行命令并判断是否成功，如不成功程序推出
        System.exit(job.waitForCompletion(true)?0:1);
    }

    public static class MergeMapper extends Mapper<LongWritable,Text,Text,Text>{

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] split = value.toString().split(",");
            //hdfs文件系统
            FileSplit fileSplit = (FileSplit)context.getInputSplit();
            //获得全路径
            String path = fileSplit.getPath().toString();
            //判断数据出自那个文件
            if (path.contains("bank")) {
                split[1]="b"+split[1];//银行信贷标示
            }
            if (path.contains("estate")) {
                split[1]="e"+split[1];//房地产投资标示
            }
            if (path.contains("network")) {
                split[1]="n"+split[1];//互联网购物标示
            }
            //写入输出文件
            context.write(new Text(split[0]),new Text(split[1]));
        }
    }

    public static class MergeReducer extends Reducer<Text,Text,NullWritable,Text>{
        //新建用来存放缓存文件数据的容器
        HashMap<String, String> dataMap = new HashMap<String, String>();
        @Override
        protected void setup(Context context) throws IOException {
            //获取hdfs缓存文件map.txt
            Path[] cacheFiles = context.getLocalCacheFiles();
            // 设置hdfs输入流对象
            BufferedReader br = new BufferedReader(new FileReader(cacheFiles[0].toUri().getPath()));
            String line="";
            //循环读取map.txt数据
            while (StringUtils.isNotEmpty(line = br.readLine())) {
                //将每条数据拆分
                String[] tmp = line.split(",");
                dataMap.put(tmp[0].trim(), String.valueOf((long)(Double.parseDouble(tmp[1])*100000000)));
            }
            System.out.println(dataMap);
            br.close();
        }

        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            //设置三个文件获取对应数据的变量
            String bankData = "";
            String estateData = "";
            String networkData = "";
            //遍历key（省份）对应数据集
            for (Text value:
                    values) {
                //通过数据标示判断数据来源，赋值给对应的变量
                if (value.toString().contains("b")){
                    bankData = value.toString().substring(value.toString().indexOf("b")+1);
                }
                if (value.toString().contains("e")){
                    estateData = value.toString().substring(value.toString().indexOf("e")+1);
                }
                if (value.toString().contains("n")){
                    networkData = value.toString().substring(value.toString().indexOf("n")+1);
                }
            }
            //组合输出文件数据字段格式
            String str = key.toString()+","+(Long.parseLong(dataMap.get(key.toString()))*100/100000000/100.0)+","+(Long.parseLong(bankData)*100/100000000/100.0)+","+(Long.parseLong(estateData)*100/100000000/100.0)+","+(Long.parseLong(networkData)*100/100000000/100.0);
            //写入输出文件
            context.write(NullWritable.get(), new Text(str));
        }
    }

}
