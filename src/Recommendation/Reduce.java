package Recommendation;

import java.io.IOException;
import java.util.ArrayList;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import java.util.TreeMap;


import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

import org.apache.hadoop.mapreduce.Reducer;



public class Reduce extends Reducer<LongWritable, FriendCountWritable, LongWritable, Text> {
    @Override
    public void reduce(LongWritable key, Iterable<FriendCountWritable> values, Context context)
            throws IOException, InterruptedException {

        // key is the recommended friend, and value is the list of mutual friends
        final java.util.Map<Long, List> mutualFriends = new HashMap<Long, List>();

        for (FriendCountWritable val : values) {
            final Boolean isAlreadyFriend = (val.mutualFriend == -1);
            final Long toUser = val.user;
            final Long mutualFriend = val.mutualFriend;

            if (mutualFriends.containsKey(toUser)) {
                if (isAlreadyFriend) {
                    mutualFriends.put(toUser, null);
                } else if (mutualFriends.get(toUser) != null) {
                    mutualFriends.get(toUser).add(mutualFriend);
                }
            } else {
                if (!isAlreadyFriend) {
                    mutualFriends.put(toUser, new ArrayList() {
                        {
                            add(mutualFriend);
                        }
                    });
                } else {
                    mutualFriends.put(toUser, null);
                }
            }
        }
         
        Map<Long, List> unsortedMutualFriends = new HashMap<Long, List>();
        ValueComparator bvc =  new ValueComparator(unsortedMutualFriends);
		TreeMap<Long, List> sortedMutualFriends=new TreeMap<>(bvc);
        
        for (java.util.Map.Entry<Long, List> entry : mutualFriends.entrySet()) {
            if (entry.getValue() != null) {
                unsortedMutualFriends.put(entry.getKey(), entry.getValue());
            }
        }
        
        sortedMutualFriends.putAll(unsortedMutualFriends);

        Integer i = 0;
        int n=0;
        String output = "";
        
        	for (java.util.Map.Entry<Long, List> entry : sortedMutualFriends.entrySet()) {
            	
       		 if (i == 0) {
                    output = entry.getKey().toString();
                } else {
                    output += "," + entry.getKey().toString() ;
                }
                ++i;	
                if(i>=10){break;}
       	     }
        
        
        
        context.write(key, new Text(output));
    }
}
