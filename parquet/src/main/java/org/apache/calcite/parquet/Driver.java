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

import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

/**
 * Driver.
 */
public class Driver {
  private Driver() {

  }

  public static void main(String... args) throws Exception {
    String jsonPath = Thread.currentThread()
        .getContextClassLoader().getResource("test-model.json").getPath();
    String url = "jdbc:calcite:model=" + jsonPath;

    Properties properties = new Properties();
    properties.put("lex", "JAVA");
    Connection conn = DriverManager.getConnection(url, properties);
/*
    String sql = "SELECT "
        + " registration_dttm,"
        + " id, "
        + " first_name, "
        + " last_name, "
        + " email, "
        + " gender, "
        + " ip_address, "
        + " cc, "
        + " country, "
        + " birthdate, "
        + " salary, "
        + " title "
        //+ " comments "
        + " from test.example";*/
    String sql = "select name from test.example";

    try (ResultSet rs = conn.createStatement().executeQuery(sql)) {
      int columnCount = rs.getMetaData().getColumnCount();
      List<String> columns = new ArrayList<>();
      for (int i = 1; i < columnCount + 1; i++) {
        String columnLabel = rs.getMetaData().getColumnLabel(i);
        System.out.println(i + " : " + rs.getMetaData().getColumnType(i));
        columns.add(columnLabel);
      }
      System.out.println(columnCount + " columns " + columns);
      int rawCounter = 0;
      while (rs.next()) {
        List<String> resultRow = new ArrayList<>();
        for (int i = 1; i < columnCount + 1; i++) {
          Object object;
    //      if (i == 1) {
////            object = rs.getBigDecimal(i);
      //    } else {
          System.out.println("class name " + rs.getMetaData().getColumnClassName(i));
          System.out.println("type name " + rs.getMetaData().getColumnTypeName(i));
          System.out.println("type name " + rs.getMetaData().getColumnTypeName(i));
          System.out.println("column_number " + i);
          object = rs.getObject(i);
        //  }

          System.out.println("object at [" + i + "] is "
              + (object == null ? "null" : object.getClass().isArray()
                  ? new String((byte[]) object, StandardCharsets.UTF_8) : String.valueOf(object)));
          resultRow.add(String.valueOf(object));
        }
        rawCounter++;
        System.out.println(resultRow);
      }
      System.out.println(rawCounter + " rows");
    }
  }
}

// End Driver.java
