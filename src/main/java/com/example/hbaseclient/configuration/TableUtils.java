package com.example.hbaseclient.configuration;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Table;

import java.io.IOException;

public class TableUtils {

    public static Connection getConnection() throws IOException {
        Configuration configuration = HBaseConfiguration.create();
        configuration.set("hbase.zookeeper.quorum", "hadoop101,hadoop102,hadoop103");
        configuration.set("hbase.zookeeper.property.clientPort", "2181");
        Connection connection = ConnectionFactory.createConnection(configuration);
        return connection;
    }

    public static Table getHTable(String tableName) throws IOException {
        Connection connection = getConnection();
        Table table = connection.getTable(TableName.valueOf(tableName));
        return table;
    }
}
