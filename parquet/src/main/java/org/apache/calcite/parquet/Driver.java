package org.apache.calcite.parquet;

import de.vandermeer.asciitable.AsciiTable;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

public class Driver {

    public static void main(String...args) throws Exception {
        String jsonPath = Thread.currentThread().getContextClassLoader().getResource("test-model.json").getPath();
        String url = "jdbc:calcite:model="+jsonPath;

        Properties properties = new Properties();
        properties.put("lex","JAVA");
        Connection conn = DriverManager.getConnection(url, properties);

        String sql = "SELECT " +
   //             "registration_dttm," +
   //             "id, " +
                "first_name, " +
                "last_name, "
                 + " email, "
        + " gender, "
        + " ip_address, "
        + " cc, "
        + " country, "
        + " birthdate, "
     //   + " salary, "
        + " title, "
        + " comments from test.example WHERE last_name = 'Adams' order by last_name";
        AsciiTable at = new AsciiTable();


        try(ResultSet rs = conn.createStatement().executeQuery(sql)) {

            at.addRule();
            int columnCount = rs.getMetaData().getColumnCount();
            List<String> columns = new ArrayList<>();
            for (int i=1; i<columnCount+1;i++) {
                String columnLabel = rs.getMetaData().getColumnLabel(i);
                columns.add(columnLabel);
            }
            at.addRow(columns);
            while (rs.next()) {
                at.addRule();
                List<String> resultRow = new ArrayList<>();
                for (int i=1;i<columnCount+1;i++) {
                    Object object = rs.getObject(i);
                    resultRow.add(object.toString());
                }
                at.addRow(resultRow);

            }
            at.addRule();
        }
        System.out.println(at.render());
    }
}