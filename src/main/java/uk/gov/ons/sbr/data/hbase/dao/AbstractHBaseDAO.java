package uk.gov.ons.sbr.data.hbase.dao;

import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Connection;
import uk.gov.ons.sbr.data.hbase.HBaseConnector;

import java.io.IOException;

/**
 * DAO with a connection to HBase
 */
class AbstractHBaseDAO {

    private Connection connection;

    Connection getConnection() throws IOException {
        if (connection != null) return connection;
        connection = this.connection = HBaseConnector.getInstance().getConnection();
        return connection;
    }

    void setConnection(Connection connection) {
        this.connection = connection;
    }
}
