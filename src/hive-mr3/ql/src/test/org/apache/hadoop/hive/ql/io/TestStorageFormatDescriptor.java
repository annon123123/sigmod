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

package org.apache.hadoop.hive.ql.io;

import org.junit.Assert;

import org.junit.Test;

import com.google.common.collect.Sets;

public class TestStorageFormatDescriptor {

  @Test
  public void testNames() {
    Assert.assertEquals(Sets.newHashSet(IOConstants.TEXTFILE),
        (new TextFileStorageFormatDescriptor()).getNames());
    Assert.assertEquals(Sets.newHashSet(IOConstants.SEQUENCEFILE),
        (new SequenceFileStorageFormatDescriptor()).getNames());
    Assert.assertEquals(Sets.newHashSet(IOConstants.RCFILE),
        (new RCFileStorageFormatDescriptor()).getNames());
    Assert.assertEquals(Sets.newHashSet(IOConstants.ORC, IOConstants.ORCFILE),
        (new ORCFileStorageFormatDescriptor()).getNames());
    Assert.assertEquals(Sets.newHashSet(IOConstants.PARQUET, IOConstants.PARQUETFILE),
        (new ParquetFileStorageFormatDescriptor()).getNames());
    Assert.assertEquals(Sets.newHashSet(IOConstants.AVRO, IOConstants.AVROFILE),
      (new AvroStorageFormatDescriptor()).getNames());
  }
}
