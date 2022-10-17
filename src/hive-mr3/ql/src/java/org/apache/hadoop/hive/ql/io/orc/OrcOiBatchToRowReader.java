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
package org.apache.hadoop.hive.ql.io.orc;

import java.util.List;

import org.apache.hadoop.hive.ql.io.AcidInputFormat.AcidRecordReader;
import org.apache.hadoop.hive.ql.io.RecordIdentifier;
import org.apache.hadoop.hive.ql.io.RecordIdentifier.Field;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.ql.io.BatchToRowReader;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatchCtx;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapred.RecordReader;

/** BatchToRowReader that returns the rows readable by ORC IOs. */
public class OrcOiBatchToRowReader extends BatchToRowReader<OrcStruct, OrcUnion>
    implements AcidRecordReader<NullWritable, Object> {

  private final RecordIdentifier recordIdentifier;
  private boolean isNull;

  public OrcOiBatchToRowReader(RecordReader<NullWritable, VectorizedRowBatch> vrbReader,
      VectorizedRowBatchCtx vrbCtx, List<Integer> includedCols) {
    super(vrbReader, vrbCtx, includedCols);
    this.recordIdentifier = new RecordIdentifier();
    this.isNull = true;
  }

  @Override
  protected OrcStruct createStructObject(Object previous, List<TypeInfo> childrenTypes) {
    int numChildren = childrenTypes.size();
    if (previous == null || !(previous instanceof OrcStruct)) {
      return new OrcStruct(numChildren);
    }
    OrcStruct result = (OrcStruct) previous;
    result.setNumFields(numChildren);
    return result;
  }

  @Override
  protected int getStructLength(OrcStruct structObj) {
    return structObj.getNumFields();
  }

  @Override
  protected OrcUnion createUnionObject(List<TypeInfo> childrenTypes, Object previous) {
    return (previous instanceof OrcUnion) ? (OrcUnion)previous : new OrcUnion();
  }

  @Override
  protected void setStructCol(OrcStruct structObj, int i, Object value) {
    structObj.setFieldValue(i, value);
  }

  @Override
  protected Object getStructCol(OrcStruct structObj, int i) {
    return structObj.getFieldValue(i);
  }

  @Override
  protected Object getUnionField(OrcUnion unionObj) {
    return unionObj.getObject();
  }

  @Override
  protected void setUnion(OrcUnion unionObj, byte tag, Object object) {
    unionObj.set(tag, object);
  }

  @Override
  protected void populateRecordIdentifier(OrcStruct rowId) {
    if (rowId == null) {
      this.isNull = true;
      return;
    }
    recordIdentifier.setValues(((LongWritable) rowId.getFieldValue(Field.writeId.ordinal())).get(),
        ((IntWritable) rowId.getFieldValue(Field.bucketId.ordinal())).get(),
        ((LongWritable) rowId.getFieldValue(Field.rowId.ordinal())).get());
    this.isNull = false;
  }

  @Override
  public RecordIdentifier getRecordIdentifier() {
    return this.isNull ? null : recordIdentifier;
  }

}
