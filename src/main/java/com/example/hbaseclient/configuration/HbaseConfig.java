package com.example.hbaseclient.configuration;

import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Table;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.io.IOException;
import java.util.Map;

@Configuration
public class HbaseConfig {

    @Value("${hbase.zookeeper.quorum}")
    private String quorum;

    @Value("${hbase.zookeeper.property.clientPort}")
    private String port;

    @Bean
    public Admin getHbaseAdmin() throws IOException {
        Connection connection = getConnection();
        Admin admin = connection.getAdmin();
        return admin;
    }

    @Bean
    public Connection getConnection() throws IOException {
        org.apache.hadoop.conf.Configuration configuration = HBaseConfiguration.create();
        configuration.set("hbase.zookeeper.quorum", quorum);
        configuration.set("hbase.zookeeper.property.clientPort", port);
        Connection connection = ConnectionFactory.createConnection(configuration);
        return connection;
    }

}
