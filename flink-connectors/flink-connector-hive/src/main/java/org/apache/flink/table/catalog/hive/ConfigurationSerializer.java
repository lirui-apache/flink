/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.table.catalog.hive;

import org.apache.hadoop.conf.Configuration;
import org.apache.hive.com.esotericsoftware.kryo.Kryo;
import org.apache.hive.com.esotericsoftware.kryo.KryoException;
import org.apache.hive.com.esotericsoftware.kryo.Serializer;
import org.apache.hive.com.esotericsoftware.kryo.io.Input;
import org.apache.hive.com.esotericsoftware.kryo.io.Output;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

/**
 * A Kryo Serializer for Configuration.
 */
public class ConfigurationSerializer extends Serializer<Configuration> {

	@Override
	public void write(Kryo kryo, Output output, Configuration configuration) {
		byte[] bytes = confToBytes(configuration);
		output.writeInt(bytes.length);
		output.writeBytes(bytes);
	}

	@Override
	public Configuration read(Kryo kryo, Input input, Class<Configuration> aClass) {
		int len = input.readInt();
		byte[] bytes = new byte[len];
		input.readBytes(bytes);
		return bytesToConf(bytes, aClass);
	}

	private static byte[] confToBytes(Configuration conf) {
		ByteArrayOutputStream byteStream = new ByteArrayOutputStream();
		try (DataOutputStream dataOutputStream = new DataOutputStream(byteStream)) {
			conf.write(dataOutputStream);
		} catch (IOException e) {
			throw new KryoException(e);
		}
		return byteStream.toByteArray();
	}

	private static <T extends Configuration> T bytesToConf(byte[] bytes, Class<T> clz) {
		try (DataInputStream dataInputStream = new DataInputStream(new ByteArrayInputStream(bytes))) {
			T conf = clz.newInstance();
			conf.readFields(dataInputStream);
			return conf;
		} catch (Exception e) {
			throw new KryoException(e);
		}
	}
}
