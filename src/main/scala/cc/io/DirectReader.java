/*
 * PACC: Partition-Aware Connected Components
 * Authors: Ha-Myung Park, Namyong Park, Sung-Hyun Myaeng, and U Kang
 *
 * Copyright (c) 2018, Ha-Myung Park, Namyong Park, Sung-Hyun Myaeng, and U Kang
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are met:
 *     * Redistributions of source code must retain the above copyright
 *       notice, this list of conditions and the following disclaimer.
 *     * Redistributions in binary form must reproduce the above copyright
 *       notice, this list of conditions and the following disclaimer in the
 *       documentation and/or other materials provided with the distribution.
 *     * Neither the name of the Seoul National University nor the
 *       names of its contributors may be used to endorse or promote products
 *       derived from this software without specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND
 * ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
 * WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
 * DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDERS BE LIABLE FOR ANY
 * DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES
 * (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES;
 * LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND
 * ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
 * (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS
 * SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 *
 * -------------------------------------------------------------------------
 * File: DirectReader.java
 * - A fast binary file reader.
 */

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

	/**
	 * read 32-bit data as an integer type value.
	 * @return 32-bit data in an integer value
	 * @throws IOException
	 */
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

	/**
	 * read 64-bit data as a long type value.
	 * @return 64-bit data in a long type value.
	 * @throws IOException
	 */
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

	/**
	 * check there is more data to read.
	 * @return true if data remain, false otherwise
	 * @throws IOException
	 */
	public boolean hasNext() throws IOException{
		return buf.hasRemaining() || (ch.size() > ch.position());
	}

	/**
	 * close this reader.
	 * @throws IOException
	 */
	public void close() throws IOException {
		in.close();
		ch.close();
		buf = null;
	}

	
}
