package cc.io;

import org.apache.log4j.Logger;

import java.io.*;
import java.nio.BufferOverflowException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Paths;

public class DirectWriter implements Closeable {

	private static final Logger LOGGER = Logger.getLogger(DirectWriter.class.getName());
	
	RandomAccessFile in;
	FileChannel ch;
	ByteBuffer buf;
	
	String path;
	
	public DirectWriter(String path) throws IOException {
		this(path, 65536);
	}
	
	private void flush() throws IOException{
		buf.flip();
		ch.write(buf);
		buf.clear();
	}
	
	public DirectWriter(String path, int bufferSize) throws IOException {
		
		Files.createDirectories(Paths.get(path).getParent());
		Files.deleteIfExists(Paths.get(path));
		in = new RandomAccessFile(path, "rw");
		ch = in.getChannel();
		try{
			buf = ByteBuffer.allocateDirect(bufferSize);
		} catch (OutOfMemoryError e){
			LOGGER.warn("Failed to allocate a direct buffer. Use a general byte buffer instead.");
			
			buf = ByteBuffer.allocate(bufferSize);
		}
		
		buf.clear();
		
		this.path = path;
	}
	
	public String getPath(){
		return path;
	}
	
	public void write(int u) throws IOException{

		try{
			buf.putInt(u);
		}catch(BufferOverflowException e){
			flush();
			buf.putInt(u);
		}
		
	}

	public void write(long u) throws IOException{

		try{
			buf.putLong(u);
		}catch(BufferOverflowException e){
			flush();
			buf.putLong(u);
		}

	}

	public void write(Object obj) throws IOException{

		if(obj instanceof Long){
			write(((Long) obj).longValue());
			return;
		}
		else if(obj instanceof Integer){
			write(((Integer) obj).intValue());
			return;
		}

		ByteArrayOutputStream baos = null;
		ObjectOutputStream oos = null;

		try{
			baos = new ByteArrayOutputStream();
			oos = new ObjectOutputStream(baos);

			oos.writeObject(obj);
			oos.flush();

			byte[] bytes = baos.toByteArray();
			int i = 0;
			while(i < bytes.length){
				int remainingBytes = bytes.length - i;
				int lengthToWrite = Math.min(buf.remaining(), remainingBytes);
				buf.put(bytes, i, lengthToWrite);
				i += lengthToWrite;
				if(buf.remaining() == 0){
					flush();
				}
			}
		} finally {
			if(oos != null) oos.close();
			if(baos != null) baos.close();
		}

	}
	

	
	public void close() throws IOException {

		flush();
		
		ch.close();
		in.close();
		buf = null;
	}
	
	
	
}
