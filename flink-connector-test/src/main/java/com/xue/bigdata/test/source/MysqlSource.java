package com.xue.bigdata.test.source;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;

/**
 * @author: mingway
 * @date: 2021/11/19 1:45 下午
 */
public class MysqlSource extends RichSourceFunction<Tuple2<String, String>> {

    private Connection connect;
    private PreparedStatement ps;

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        Class.forName("com.mysql.jdbc.Driver");
        connect = DriverManager.getConnection("jdbc:mysql://am-wz9ld63fq4oa7396v90650.ads.aliyuncs.com:3306/bigdata?charset=utf8&autocommit=true&rewriteBatchedStatements=true", "bigdata_pub_bi", "%QqHU&#Xhocb14pS");
        ps = connect.prepareStatement("select order_id,comment from bigdata.dwd_comments_info where comment <> '' limit 100");
    }

    @Override
    public void run(SourceContext<Tuple2<String, String>> ctx) throws Exception {
        ResultSet rs = ps.executeQuery();
        while (rs.next()) {
            ctx.collect(new Tuple2<String, String>(rs.getString(1), rs.getString(2)));
        }
    }

    @Override
    public void cancel() {
        try {
            super.close();
            if (connect != null) {
                connect.close();
            }
            if (ps != null) {
                ps.close();
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
