package com.xue.bigdata.test.sql;

import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.parser.SqlParser;

public class CalciteTest {
    public static void main(String[] args) throws Exception {
        SqlParser parser = SqlParser.create("select * from source", SqlParser.Config.DEFAULT);
        SqlNode sqlNode = parser.parseStmt();

        System.out.println();
    }
}
