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

import org.apache.calcite.plan.RelTrait;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.hive.com.esotericsoftware.kryo.Kryo;
import org.apache.hive.com.esotericsoftware.kryo.Serializer;
import org.apache.hive.com.esotericsoftware.kryo.io.Input;
import org.apache.hive.com.esotericsoftware.kryo.io.Output;

/**
 * A Kryo Serializer for RelTraitSet.
 */
public class RelTraitSetSerializer extends Serializer<RelTraitSet> {

	@Override
	public void write(Kryo kryo, Output output, RelTraitSet relTraits) {
		output.writeInt(relTraits.size());
		for (RelTrait relTrait : relTraits) {
			kryo.writeClassAndObject(output, relTrait);
		}
	}

	@Override
	public RelTraitSet read(Kryo kryo, Input input, Class<RelTraitSet> aClass) {
		int size = input.readInt();
		RelTraitSet relTraitSet = RelTraitSet.createEmpty();
		for (int i = 0; i < size; i++) {
			RelTrait relTrait = (RelTrait) kryo.readClassAndObject(input);
			relTraitSet.plus(relTrait);
		}
		return relTraitSet;
	}
}
