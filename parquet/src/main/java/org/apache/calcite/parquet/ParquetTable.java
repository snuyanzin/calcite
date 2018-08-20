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

import org.apache.calcite.adapter.java.AbstractQueryableTable;
import org.apache.calcite.linq4j.AbstractEnumerable;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.linq4j.Enumerator;
import org.apache.calcite.linq4j.QueryProvider;
import org.apache.calcite.linq4j.Queryable;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rel.type.RelDataTypeImpl;
import org.apache.calcite.rel.type.RelDataTypeSystem;
import org.apache.calcite.rel.type.RelProtoDataType;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.TranslatableTable;
import org.apache.calcite.sql.type.SqlTypeFactoryImpl;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.util.Pair;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;

import org.apache.parquet.format.converter.ParquetMetadataConverter;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.Type;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * ParquetTable.
 */
public class ParquetTable extends AbstractQueryableTable implements TranslatableTable {

  private final File file;

  public ParquetTable(File rootDir) {
    super(Object[].class);
    this.file = rootDir;
  }

  @Override public RelDataType getRowType(RelDataTypeFactory typeFactory) {
    try {
      ParquetMetadata parquetMetadata =
          ParquetFileReader.readFooter(
              new Configuration(), new Path(file.toURI()), ParquetMetadataConverter.NO_FILTER);
      MessageType schema = parquetMetadata.getFileMetaData().getSchema();
      List<Type> fields = schema.getFields();
      final List<RelDataType> types = new ArrayList<>();
      final List<String> names = new ArrayList<>();
      for (Type field : fields) {
        System.out.println("fieldName: "
            + field.getName() + " is primitive "  + field.isPrimitive());
        System.out.println("repetition: " + field.getRepetition());
        names.add(field.getName());
        if (field.isPrimitive()) {
          PrimitiveType.PrimitiveTypeName primitiveTypeName =
              field.asPrimitiveType().getPrimitiveTypeName();
          System.out.println("primitiveTypeName: " + primitiveTypeName);
          System.out.println("primitiveTypeName javaType: " + primitiveTypeName.javaType);
          switch (primitiveTypeName) {
          case INT96:
            types.add(
                typeFactory.createArrayType(typeFactory.createSqlType(SqlTypeName.DECIMAL), 12));
            break;
          case INT64:
            types.add(typeFactory.createSqlType(SqlTypeName.BIGINT));
            break;
          case INT32:
            types.add(typeFactory.createSqlType(SqlTypeName.INTEGER));
            break;
          case FLOAT:
            types.add(typeFactory.createSqlType(SqlTypeName.FLOAT));
            break;
          case BINARY:
            types.add(typeFactory.createSqlType(SqlTypeName.VARCHAR));
            break;
          case DOUBLE:
            types.add(typeFactory.createSqlType(SqlTypeName.DOUBLE));
            break;
          case BOOLEAN:
            types.add(typeFactory.createSqlType(SqlTypeName.BOOLEAN));
            break;
          }
        } else {
          System.out.println("fields " + field.asGroupType().getFields());
          if (field.asGroupType().getFields().size() == 1) {
            types.add(typeFactory.createSqlType(SqlTypeName.VARCHAR));
          } else {
            types.add(typeFactory.createSqlType(SqlTypeName.VARCHAR));
          }
        }

      }
      return typeFactory.createStructType(Pair.zip(names, types));
    } catch (Exception e) {
      e.printStackTrace();
    }
    return null;
  }

  public Enumerable<Object> runQuery(final List<String> fields, final String predicate) {
    final RelDataTypeFactory typeFactory = new SqlTypeFactoryImpl(RelDataTypeSystem.DEFAULT);
    final RelDataTypeFactory.Builder fieldInfo = typeFactory.builder();
    final RelDataType rowType = getRowType(typeFactory);

    if (fields.isEmpty()) {
      for (RelDataTypeField relDataTypeField : rowType.getFieldList()) {
        fieldInfo.add(relDataTypeField);
      }
    } else {
      for (String field : fields) {
        fieldInfo.add(rowType.getField(field, true, false));
      }
    }
    final RelProtoDataType resultRowType = RelDataTypeImpl.proto(fieldInfo.build());

    return new AbstractEnumerable<Object>() {
      public Enumerator<Object> enumerator() {
        return new ParquetEnumerator(file, new AtomicBoolean(false), resultRowType, predicate);
      }
    };
  }

  @Override public RelNode toRel(RelOptTable.ToRelContext context, RelOptTable relOptTable) {
    List<String> fieldNames = relOptTable.getRowType().getFieldNames();
    return new ParquetTableScan(context.getCluster(),
        context.getCluster().traitSetOf(ParquetRel.CONVENTION),
        relOptTable, this, relOptTable.getRowType());
  }

  @Override public <T> Queryable<T> asQueryable(
          QueryProvider queryProvider, SchemaPlus schema, String tableName) {
    throw new UnsupportedOperationException();
  }
}

// End ParquetTable.java
