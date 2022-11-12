package friendRecommend2;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class FriendRecommend2 {
    public static class FriendMapper2 extends Mapper<LongWritable, Text, Text, IntWritable> {
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            // value 形如tom+制表符+lucy smith jim rose的结果
            String[] strings = value.toString().split("\t");
            String name = strings[0];// tom
            String friends = strings[1];// lucy smith jim rose
            // 拆分得到好友列表
            String[] s = friends.split(" ");
            // name和所有好友列表都是好友，输出拼接后好友为key，0为value，0代表不推荐，本身就是好友
            for (String friend : s) {
                String fkey = getKey(name, friend);
                context.write(new Text(fkey), new IntWritable(0));
            }
            // 其他好友列表之间，因为有共同好友，value为1
            for (int i = 0; i < s.length; i++) {
                for (int j = i + 1; j < s.length; j++) {
                    String fkey = getKey(s[i], s[j]);
                    context.write(new Text(fkey), new IntWritable(1));
                }
            }

        }

        // 写一个方法，拼接好友
        public static String getKey(String name1, String name2) {
            if (name1.compareTo(name2) > 0) {
                return name1 + ":" + name2;
            } else {
                return name2 + ":" + name1;
            }
        }

        public static class FriendReducer2 extends Reducer<Text, IntWritable,Text, IntWritable> {
            @Override
            protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
                //key就是心如clyang:messi这样的字符串
                //values就是[0 1 0 1 0]这样的结果，只要有0，就不写出，不推荐
                boolean flag=true;
                //共同好友个数
                int num=0;
                for (IntWritable value : values) {
                    if(value.get()==0){
                        flag=false;
                    }
                    num++;
                }
                //如果flag为false，不推荐,否则输出，value为共同好友数
                if(flag){
                    context.write(key,new IntWritable(num));
                }
            }

        public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        //向yarn提交一个job
        Job job=Job.getInstance(new Configuration());
        //设置入口类
        job.setJarByClass(FriendMain2.class);

        //设置mapper类和reducer类
        job.setMapperClass(FriendMapper2.class);
        job.setReducerClass(FriendReducer2.class);

        //设置map输出格式
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        //设置reduce输出格式
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        //如果map输出格式和reduce输出格式一样，就只需要设置reduce就行

        //设置读取的文件，为上次输出的路径
        FileInputFormat.addInputPath(job,new Path("hdfs://localhost:9000/output/friendout/"));
        //设置输出路径
        //输出路径必须不存在
        FileOutputFormat.setOutputPath(job,new Path("hdfs://localhost:9000/new_output/friendrecommend"));
        //提交
        job.waitForCompletion(true);
    }
}