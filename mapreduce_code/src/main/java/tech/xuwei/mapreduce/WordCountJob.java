package tech.xuwei.mapreduce;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * 需求：读取hdfs上的hello.txt文件，计算文件中每个单词出现的总次数
 *
 * 原始文件hello.txt内容如下：
 * hello you
 * hello me
 *
 * 最终需要的结果形式如下：
 * hello    2
 * me   1
 * you  1
 *
 * Created by xuwei
 */
public class WordCountJob {
    /**
     * Map阶段
     */
    public static class MyMapper extends Mapper<LongWritable,Text,Text, LongWritable>{
        /**
         * 需要实现map函数
         * map函数接收的是<k1,v1>，输出的是<k2,v2>
         * @param k1
         * @param v1
         * @param context
         * @throws IOException
         * @throws InterruptedException
         */
        @Override
        protected void map(LongWritable k1, Text v1, Context context)
                throws IOException, InterruptedException {
            // k1：每一行数据的行首偏移量
            // v1：每一行数据
            // 在这里对获取到的每一行数据进行拆分，把单词拆分出来
            String[] words = v1.toString().split(" ");
            // 迭代拆分出来的单词数据
            for (String word:words) {
                // 把迭代后的单词封装成<k2,v2>的形式
                Text k2 = new Text(word);
                LongWritable v2 = new LongWritable(1L);
                // 把<k2,v2>写出去
                context.write(k2,v2);
            }
        }
    }


    /**
     * Reduce阶段
     */
    public static class MyReducer extends Reducer<Text,LongWritable,Text, LongWritable>{
        /**
         * 针对v2s（多个v2）数据进行累加求和，并最终把数据转换为<k3,v3>写出去
         * @param k2
         * @param v2s
         * @param context
         * @throws IOException
         * @throws InterruptedException
         */
        @Override
        protected void reduce(Text k2, Iterable<LongWritable> v2s, Context context)
                throws IOException, InterruptedException {
            // 创建一个sum变量，保存v2s的和
            long sum = 0L;
            for (LongWritable v2 : v2s) {
                sum += v2.get();
            }
            // 组装<k3,v3>
            Text k3 = k2;
            LongWritable v3 = new LongWritable(sum);
            // 把结果写出去
            context.write(k3,v3);
        }
    }

    /**
     * 组装Job=Map+Reduce
     */
    public static void main(String[] args) {
        try{
            if(args.length!=2){
                //如果传递的参数不够，程序直接退出
                System.exit(100);
            }

            //指定Job需要的配置参数
            Configuration conf = new Configuration();
            //创建一个Job
            Job job = Job.getInstance(conf);

            //这一行必须设置，否则在集群中执行的时候是找不到WordCountJob这个类的
            job.setJarByClass(WordCountJob.class);

            //指定输入路径（可以是文件，也可以是目录）
            FileInputFormat.setInputPaths(job,new Path(args[0]));
            //指定输出路径(只能指定一个不存在的目录)
            FileOutputFormat.setOutputPath(job,new Path(args[1]));

            //指定map相关的代码
            job.setMapperClass(MyMapper.class);
            //指定k2的类型
            job.setMapOutputKeyClass(Text.class);
            //指定v2的类型
            job.setMapOutputValueClass(LongWritable.class);


            //指定reduce相关的代码
            job.setReducerClass(MyReducer.class);
            //指定k3的类型
            job.setOutputKeyClass(Text.class);
            //指定v3的类型
            job.setOutputValueClass(LongWritable.class);

            //提交job
            job.waitForCompletion(true);
        }catch(Exception e){
            e.printStackTrace();
        }

    }
}
