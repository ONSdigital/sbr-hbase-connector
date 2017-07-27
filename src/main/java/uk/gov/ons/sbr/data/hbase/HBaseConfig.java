package uk.gov.ons.sbr.data.hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;

import java.io.IOException;

public class HBaseConfig {
    private Configuration hbaseConf;

    public HBaseConfig() {
        this.hbaseConf = HBaseConfiguration.create();
    }

    public HBaseConfig(Configuration hbaseConf) {
        this.hbaseConf = hbaseConf;
    }

    public Configuration getConfig() {
        return hbaseConf;
    }

    public Connection getConnection() throws IOException {
        return ConnectionFactory.createConnection(hbaseConf);
    }
}