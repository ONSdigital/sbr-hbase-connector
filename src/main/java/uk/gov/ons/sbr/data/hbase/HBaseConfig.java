package uk.gov.ons.sbr.data.hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;

import java.io.IOException;

public class HBaseConfig {
    private static final String HBASE_CONFIGURATION_ZOOKEEPER_QUORUM = "hbase.zookeeper.quorum";
    private static final String HBASE_CONFIGURATION_ZOOKEEPER_CLIENTPORT = "hbase.zookeeper.property.clientPort";
    private Configuration hbaseConf;

    public HBaseConfig() {
        this.hbaseConf = HBaseConfiguration.create();
    }

    public HBaseConfig(Configuration config) {
        this.hbaseConf = config;
    }

    public HBaseConfig(String hbaseZookeeperQuorum, int hbaseZookeeperClientPort) {
        this();
        hbaseConf.set(HBASE_CONFIGURATION_ZOOKEEPER_QUORUM, hbaseZookeeperQuorum);
        hbaseConf.setInt(HBASE_CONFIGURATION_ZOOKEEPER_CLIENTPORT, hbaseZookeeperClientPort);
    }

    public HBaseConfig(String hbaseZookeeperQuorum) {
        this(hbaseZookeeperQuorum, 2181);
    }

    public Configuration getConfig() {
        return hbaseConf;
    }

    public Connection getConnection() throws IOException {
        return ConnectionFactory.createConnection(hbaseConf);
    }
}