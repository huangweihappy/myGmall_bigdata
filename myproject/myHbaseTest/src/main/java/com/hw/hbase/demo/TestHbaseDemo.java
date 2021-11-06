package com.hw.hbase.demo;

import com.sun.javafx.logging.PulseLogger;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.NamespaceDescriptor;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;

import java.io.IOException;

public class TestHbaseDemo {
    private static Connection connection;
    static {
        Configuration configuration = HBaseConfiguration.create();

            configuration.set("hbase.zookeeper.quorum","hadoop102,hadoop103,hadoop104");
        try {
           connection =  ConnectionFactory.createConnection(configuration);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static void main(String[] args) throws IOException {
        createNamespace("mydb1");
    }

    //获取collection对象
    //c从collection对象中去拿到admin方法
    public static void createNamespace(String nameSpaceName) throws IOException {
        Admin admin = connection.getAdmin();
        NamespaceDescriptor.Builder builder = NamespaceDescriptor.create(nameSpaceName);
        NamespaceDescriptor build = builder.build();
        admin.createNamespace(build);


        admin.close();

    }

}
