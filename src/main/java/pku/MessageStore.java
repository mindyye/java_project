package pku;

import pku.ByteMessage;
import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.HashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

public class MessageStore {
	static final MessageStore store = new MessageStore();
	// 消息存储
	HashMap<String, DataOutputStream> files = new HashMap<>();// 写
	HashMap<String, DataInputStream> filesin = new HashMap<>();// 读
	static AtomicInteger pushCount = new AtomicInteger();//没写完的push数量

	public void increasePro() {
		pushCount.incrementAndGet();//自增
	}

	public void flush() throws IOException {
		if (pushCount.decrementAndGet() == 0) {//所有文件close
			for (String key : files.keySet()) {
				files.get(key).close();	
			}
		}
	}

	// push
	public void push(ByteMessage msg, String topic) throws IOException {
		if (msg == null) {
			return;
		}
		DataOutputStream outtmp;
		synchronized (files) {
			if (!files.containsKey(topic)) {
				outtmp = new DataOutputStream(new BufferedOutputStream(new FileOutputStream("./data/" + topic, true)));
				files.put(topic, outtmp);
			} else {
				outtmp = files.get(topic);
			}
		}
		// 写入消息
		byte[] body = null;
		byte headNum = (byte) msg.headers().keySet().size();
		byte bytmp;
		if (msg.getBody().length > 1024) {
			body = msg2byte_gzip(msg.getBody());//压缩body
			bytmp = 1;
		} else {
			body = msg.getBody();
			bytmp = 0;
		}
		synchronized (outtmp) {
			outtmp.writeByte(headNum);
			for (String key : msg.headers().keySet()) {
				outtmp.writeByte(MessageHeaderType.headerTypeByte.get(key));
				switch (MessageHeaderType.headerType.get(key)) {
				case 1:
					outtmp.writeLong(msg.headers().getLong(key));
					break;
				case 2:
					outtmp.writeDouble(msg.headers().getDouble(key));
					break;
				case 3:
					outtmp.writeInt(msg.headers().getInt(key));
					break;
				case 4:
					outtmp.writeUTF(msg.headers().getString(key));
					break;
				}
			}
			outtmp.writeByte(bytmp);
			outtmp.writeShort(body.length);
			outtmp.write(body);
		}
	}

	public ByteMessage pull(String queue, String topic) throws IOException {
		String k = queue + " " + topic;
		DataInputStream intmp;
		if (!filesin.containsKey(k)) {
			try {
				intmp = new DataInputStream(new BufferedInputStream(new FileInputStream("./data/" + topic)));
			} catch (FileNotFoundException e) {
				// e.printStackTrace();
				return null;
			}
			synchronized (filesin) {
				filesin.put(k, intmp);
			}
		} else {
			intmp = filesin.get(k);
		}

		if (intmp.available() != 0) {
			// 读入消息
			ByteMessage msg = new DefaultMessage();
			byte headtype;
			byte headNum = intmp.readByte();
			for (int i = 0; i < headNum; i++) {
				headtype = intmp.readByte();
				switch (MessageHeaderType.headerReadTB.get(headtype)) {
				case (short) 1:
					msg.putHeaders(MessageHeaderType.headerReadIB.get(headtype), intmp.readLong());
					break;
				case (short) 2:
					msg.putHeaders(MessageHeaderType.headerReadIB.get(headtype), intmp.readDouble());
					break;
				case (short) 3:
					msg.putHeaders(MessageHeaderType.headerReadIB.get(headtype), intmp.readInt());
					break;
				case (short) 4:
					msg.putHeaders(MessageHeaderType.headerReadIB.get(headtype), intmp.readUTF());
					break;
				}
			}
			msg.putHeaders("Topic", topic);//存topic
			byte isZip = intmp.readByte();
			short l = intmp.readShort();
			byte[] d = new byte[l];
			intmp.read(d);
			if (isZip == 1) {
				msg.setBody(byte2msg_gzip(d));
			} else {
				msg.setBody(d);
			}
			return msg;
		} else {
			return null;
		}
	}
	//压缩
	public static byte[] msg2byte_gzip(byte[] data) {
		byte[] b = null;
		try {
			ByteArrayOutputStream bos = new ByteArrayOutputStream();
			GZIPOutputStream gzip = new GZIPOutputStream(bos);
			gzip.write(data);
			//gzip.finish();
			gzip.close();
			b = bos.toByteArray();
			bos.close();
		} catch (Exception ex) {
			ex.printStackTrace();
		}
		return b;
	}
	//解压缩
	public static byte[] byte2msg_gzip(byte[] data) {
		byte[] b = null;
		try {
			ByteArrayInputStream bis = new ByteArrayInputStream(data);
			GZIPInputStream gzip = new GZIPInputStream(bis);
			byte[] buf = new byte[1024];
			int num = -1;
			ByteArrayOutputStream baos = new ByteArrayOutputStream();
			while ((num = gzip.read(buf, 0, buf.length)) != -1) {
				baos.write(buf, 0, num);
			}
			b = baos.toByteArray();
			//baos.flush();
			baos.close();
			gzip.close();
			bis.close();
		} catch (Exception ex) {
			ex.printStackTrace();
		}
		return b;
	}
}
