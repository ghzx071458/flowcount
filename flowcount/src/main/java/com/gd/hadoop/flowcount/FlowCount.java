package com.gd.hadoop.flowcount;  
  
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;  
  
public class FlowCount {  
  
    static class MyMapper extends Mapper<LongWritable, Text, Text, FlowBean>  
    {  
  
        @Override  
        protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, FlowBean>.Context context)  
                throws IOException, InterruptedException {  
            // TODO Auto-generated method stub  
            String line=value.toString();  
            String[] words=line.split(" ");  
            context.write(new Text(words[0]), new FlowBean(Long.parseLong(words[1]), Long.parseLong(words[2])));  
        }         
          
    }  
      
    static class MyReducer extends Reducer<Text, FlowBean, Text, FlowBean>  
    {  
  
        @Override  
        protected void reduce(Text arg0, Iterable<FlowBean> arg1, Reducer<Text, FlowBean, Text, FlowBean>.Context arg2)  
                throws IOException, InterruptedException {  
            // TODO Auto-generated method stub  
            long sum_upFlow=0;  
            long sum_downFlow=0;  
            for (FlowBean flowBean : arg1) {  
                sum_upFlow+=flowBean.getUpFlow();  
                sum_downFlow+=flowBean.getDownFlow();  
            }  
            arg2.write(arg0, new FlowBean(sum_upFlow, sum_downFlow));  
        }  
          
    }  
    public static void main(String[] args) throws Exception {  
        // TODO Auto-generated method stub  
        //创建配置对象  
        Configuration configuration=new Configuration();  
          
        //创建Job对象  
        Job job=Job.getInstance(configuration, "FlowCount");  
          
        //指定Jar包所在的本地路径  
        job.setJarByClass(FlowCount.class);  
          
        //指定本Job任务要使用的Mapper类与Reducer类  
          
        job.setMapperClass(MyMapper.class);  
        job.setReducerClass(MyReducer.class);  
          
        //指定Mapper输出的key-value类型  
        job.setMapOutputKeyClass(Text.class);  
        job.setMapOutputValueClass(FlowBean.class);  
          
        //指定最终输出的key-value类型  
        job.setOutputKeyClass(Text.class);  
        job.setOutputValueClass(FlowBean.class);  
          
        FileInputFormat.setInputPaths(job, new Path(args[0]));  
        FileOutputFormat.setOutputPath(job, new Path(args[1]));  
        boolean flag=job.waitForCompletion(true);  
        if (flag) {  
            System.out.println("complete sucessfully");  
        }  
        else{  
              
            System.out.println("complete error");  
        }  
    }  
  
}  