package Recommendation;

import java.io.IOException;
import java.util.ArrayList;

import java.util.List;
import java.util.StringTokenizer;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

import org.apache.hadoop.mapreduce.Mapper;



public class Map extends Mapper<LongWritable, Text, LongWritable, FriendCountWritable> {
    private Text word = new Text();

    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line[] = value.toString().split("\t");
        Long fromUser = Long.parseLong(line[0]);
        List toUsers = new ArrayList();

        if (line.length == 2) {
            StringTokenizer tokenizer = new StringTokenizer(line[1], ",");
            while (tokenizer.hasMoreTokens()) {
                Long toUser = Long.parseLong(tokenizer.nextToken());
                toUsers.add(toUser);
                context.write(new LongWritable(fromUser),
                        new FriendCountWritable(toUser, -1L));
            }

            for (int i = 0; i < toUsers.size(); i++) {
                for (int j = i + 1; j < toUsers.size(); j++) {
                    context.write(new LongWritable((long) toUsers.get(i)),
                            new FriendCountWritable((Long) (toUsers.get(j)), fromUser));
                    context.write(new LongWritable((long) toUsers.get(j)),
                            new FriendCountWritable((Long) (toUsers.get(i)), fromUser));
                }
                }
            }
        }
    }