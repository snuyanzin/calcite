/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.calcite.parquet;

import org.apache.calcite.linq4j.Enumerator;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rel.type.RelDataTypeSystem;
import org.apache.calcite.rel.type.RelProtoDataType;
import org.apache.calcite.sql.type.SqlTypeFactoryImpl;

import org.apache.hadoop.fs.Path;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.parquet.tools.read.SimpleReadSupport;
import org.apache.parquet.tools.read.SimpleRecord;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * ParquetEnumerator.
 */
public class ParquetEnumerator implements Enumerator<Object> {

  private final ParquetReader<SimpleRecord> reader;
  private SimpleRecord current;
  private final AtomicBoolean cancel;
  private final List<RelDataTypeField> fieldTypes;

  public ParquetEnumerator(File fileToRead, AtomicBoolean cancel,
      RelProtoDataType protoRowType, String predicate) {
    this.cancel = cancel;
    final RelDataTypeFactory typeFactory =
        new SqlTypeFactoryImpl(RelDataTypeSystem.DEFAULT);
    this.fieldTypes = protoRowType.apply(typeFactory).getFieldList();
    try {
      this.reader =
          ParquetReader.builder(new SimpleReadSupport(), new Path(fileToRead.toURI())).build();
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  @Override public Object current() {
    Object[] row = new Object[fieldTypes.size()];

    List<SimpleRecord.NameValue> values = current.getValues();
    int i = 0;
    outer : for (RelDataTypeField fieldType : fieldTypes) {
      String name = fieldType.getName();
      RelDataType type = fieldType.getType();
      for (SimpleRecord.NameValue value : values) {
        if (value.getName().equals(name)) {
          row[i] = value.getValue();
          i++;
          continue outer;
        }
      }
      // Defaulting some values cause some are optional
      switch (type.getSqlTypeName()) {
      case DOUBLE:
        row[i] = 0.0D;
        i++;
        break;
      }
    }
    return row;
  }

  @Override public boolean moveNext() {
    if (cancel.get()) {
      return false;
    }

    try {
      current = this.reader.read();
      if (current == null) {
        return false;
      }
    } catch (IOException e) {
      e.printStackTrace();
    }
    return true;
  }

  @Override public void reset() {

  }

  @Override public void close() {
    try {
      reader.close();
    } catch (IOException e) {
      e.printStackTrace();
    }
  }
}

// End ParquetEnumerator.java
