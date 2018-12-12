package pku;

public class Producer {
	public String topic;
	protected Producer(){
		MessageStore.store.increasePro();
	}
    public ByteMessage createBytesMessageToTopic(String topic, byte[] body)throws Exception{
    		ByteMessage msg=new DefaultMessage(body);
    		this.topic=topic;
    		return msg;
    }
    public void send(ByteMessage defaultMessage)throws Exception{
        MessageStore.store.push(defaultMessage,topic);
    }
    public void flush() throws Exception{
    		MessageStore.store.flush();
    }
}
