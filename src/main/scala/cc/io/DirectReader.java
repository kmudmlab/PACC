package cc.io;

import org.apache.log4j.Logger;

import java.io.Closeable;
import java.io.EOFException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.BufferUnderflowException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

public class DirectReader implements Closeable {

	private static final Logger LOGGER = Logger.getLogger(DirectReader.class.getName());

	private RandomAccessFile in;
	private FileChannel ch;
	private ByteBuffer buf;
	
	String path;
	
	public DirectReader(String path) throws IOException{
		this(path, 65536);
	}
	
	public DirectReader(String path, int bufferSize) throws IOException {
		
		in = new RandomAccessFile(path, "r");
		ch = in.getChannel();
		try{
			buf = ByteBuffer.allocateDirect(bufferSize);
		} catch (OutOfMemoryError e){
			LOGGER.warn("Failed to allocate a direct buffer. Use a general byte buffer instead.");
			
			buf = ByteBuffer.allocate(bufferSize);
		}
		buf.flip();
		
		this.path = path;
	}
	
	public String getPath(){
		return path;
	}
	
	private void loadNext() throws IOException{
		
		buf.clear();
		ch.read(buf);
		buf.flip();
		
	}
	
	public int readInt() throws IOException{
		
		int u;
		
		try {
			u = buf.getInt();
		} catch(BufferUnderflowException e){
			
			loadNext();
		
			if(buf.hasRemaining()){
				u = buf.getInt();
			}
			else throw new EOFException();
		}
		
		return u;
		
	}
	
	public long readLong() throws IOException{
		
		long u;
		
		try {
			u = buf.getLong();
		} catch(BufferUnderflowException e){
			
			loadNext();
		
			if(buf.hasRemaining()){
				u = buf.getLong();
			}
			
			else throw new EOFException();
		}
		
		return u;
		
	}
	
	public boolean hasNext() throws IOException{

//		try {
			return buf.hasRemaining() || (ch.size() > ch.position());
//		} catch (EOFException eof){
//			return false;
//		}
	}
	
	
	public void close() throws IOException {
		in.close();
		ch.close();
		buf = null;
	}

	long markPos;
	
	public void mark() throws IOException {
		markPos = ch.position()-buf.limit()+buf.position();
	}

	public void reset() throws IOException {
		ch.position(markPos);
		loadNext();
	}
	
	public void init() throws IOException{
		ch.position(0);
		loadNext();
	}

	public void rewind(int numBytes) throws IOException {
		int bufPos = buf.position();
		
		if(bufPos >= numBytes){
			buf.position(bufPos - numBytes);
		}
		else{
			ch.position(ch.position() - buf.limit() + bufPos - numBytes);
			loadNext();
		}
	}
	
	
	
}
