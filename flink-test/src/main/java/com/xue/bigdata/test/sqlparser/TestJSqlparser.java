package com.xue.bigdata.test.sqlparser;

import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.ExpressionVisitorAdapter;
import net.sf.jsqlparser.expression.operators.conditional.AndExpression;
import net.sf.jsqlparser.parser.CCJSqlParserUtil;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.select.*;

import java.util.HashMap;
import java.util.Map;

public class TestJSqlparser {
    public static void main(String[] args) throws Exception {
        String sql = "select\n" +
                "table_schema as '数据库',\n" +
                "TABLE_NAME  as '表',\n" +
                "sum(table_rows) as '记录数',\n" +
                "sum(truncate(data_length/1024/1024/1024, 2)) as '数据容量(GB)',\n" +
                "sum(truncate(index_length/1024/1024/1024, 2)) as '索引容量(GB)'\n" +
                "from information_schema.tables as t\n" +
                "-- where TABLE_NAME = 'ods_log_action'\n" +
                "where TABLE_SCHEMA = 'bigdata'\n" +
                "and TABLE_TYPE != 'VIEW'\n" +
                "group by table_schema,TABLE_NAME\n" +
                "order by sum(data_length) desc, sum(index_length) desc;";
        Select stmt = (Select) CCJSqlParserUtil.parse(sql);

        Map<String, Expression> map = new HashMap<>();
        Map<String, String> mapTable = new HashMap<>();

        ((PlainSelect) stmt.getSelectBody()).getFromItem().accept(new FromItemVisitorAdapter() {
            @Override
            public void visit(Table table) {
                // 获取别名 => 表名
                mapTable.put(table.getAlias().getName(), table.getName());
            }
        });

        /*((PlainSelect) stmt.getSelectBody()).getWhere().accept(new ExpressionVisitorAdapter() {
            @Override
            public void visit(AndExpression expr) {
                // 获取where表达式
                System.out.println(expr);
            }
        });*/

        for (SelectItem selectItem : ((PlainSelect)stmt.getSelectBody()).getSelectItems()) {
            selectItem.accept(new SelectItemVisitorAdapter() {
                @Override
                public void visit(SelectExpressionItem item) {
                    // 获取字段别名 => 字段名
                    map.put(item.getAlias().getName(), item.getExpression());
                }
            });
        }

        System.out.println("map " + map);
        System.out.println("mapTables" + mapTable);
    }
}
