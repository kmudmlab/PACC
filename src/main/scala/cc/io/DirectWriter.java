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
 * File: DirectWriter.java
 * - A fast binary file writer.
 */

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
	
	/**
	 * write a 32-bit value.
	 * @param u 32-bit value
	 * @throws IOException
	 */
	public void write(int u) throws IOException{

		try{
			buf.putInt(u);
		}catch(BufferOverflowException e){
			flush();
			buf.putInt(u);
		}
		
	}

	/**
	 * write a 64-bit value.
	 * @param u 64-bit value
	 * @throws IOException
	 */
	public void write(long u) throws IOException{

		try{
			buf.putLong(u);
		}catch(BufferOverflowException e){
			flush();
			buf.putLong(u);
		}

	}

	/**
	 * close this writer.
	 * @throws IOException
	 */
	public void close() throws IOException {

		flush();
		
		ch.close();
		in.close();
		buf = null;
	}

	/**
	 * flush the buffer.
	 * @throws IOException
	 */
	private void flush() throws IOException{
		buf.flip();
		ch.write(buf);
		buf.clear();
	}
	
}
