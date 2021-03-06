package pku;

import java.util.Collection;
import java.util.LinkedList;
import java.util.List;

public class Consumer {
	List<String> topics = new LinkedList<>();
    int readPos = 0;
    String queue;
	
    public void attachQueue(String queueName, Collection<String> t) throws Exception {
    	 	if (queue != null) {
             throw new Exception("只允许绑定一次。。。");
         }
         queue = queueName;
         topics.addAll(t);
    }
    
    public ByteMessage poll()throws Exception{
    	 	ByteMessage re = null;
         //先读第一个topic, 再读第二个topic...
         //直到所有topic都读完了, 返回null, 表示无消息
        for (int i = 0; i < topics.size(); i++) {
        		int index = (i + readPos) % topics.size();
        		re = MessageStore.store.pull(queue, topics.get(index));
        		if (re != null) {
                readPos = index + 1;
                 break;
            }
         }
         return re;
    }
}
