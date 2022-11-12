package friendRecommend;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import com.boe.profitMapReduce.MyPartitioner;
import com.boe.profitMapReduce.Profit;
import com.boe.profitMapReduce.ProfitMapper;
import com.boe.profitMapReduce.ProfitReducer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.io.IOException;

/**
 * 先根据第一个name，分组求出它所有的朋友
 */
public class FriendRecommend {

    public static class FriendMapper extends Mapper<LongWritable, Text, Text, Text> {
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            // 拆分每一行
            String[] s = value.toString().split(" ");
            // 直接写出到map输出，第一个名字相同，会落在同一分区
            context.write(new Text(s[0]), new Text(s[1]));
        }
    }

    public class FriendReducer extends Reducer<Text, Text, Text, Text> {
        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {
            // 好友字符串，用空格隔开
            String s = "";
            for (Text text : values) {
                s += text.toString() + " ";
            }
            // 去掉最后一个空格
            String friends = s.trim();
            // 写出
            context.write(key, new Text(friends));
        }
    }

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        //向yarn提交一个job
        Job job=Job.getInstance(new Configuration());
        //设置入口类
        job.setJarByClass(FriendMain.class);

        //设置mapper类和reducer类
        job.setMapperClass(FriendMapper.class);
        job.setReducerClass(FriendReducer.class);

        //设置map输出格式
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        //设置reduce输出格式
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        //如果map输出格式和reduce输出格式一样，就只需要设置reduce就行

        //设置读取的文件
        Path("hdfs://localhost:9000/usr/input/soc-pokec-relationships-small.txt"));
        //设置输出路径
        //输出路径必须不存在
        FileOutputFormat.setOutputPath(job,new Path("hdfs://localhost:9000/output/friendout"));
        //提交
        job.waitForCompletion(true);
    }
}