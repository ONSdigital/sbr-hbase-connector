package uk.gov.ons.sbr.data.hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.security.UserGroupInformation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import uk.gov.ons.sbr.data.hbase.table.TableNames;

import java.io.File;
import java.io.IOException;

public class HBaseConnector {

    private static final String ZOOKEEPER_QUORUM = "ZOOKEEPER_QUORUM";
    private static final String ZOOKEEPER_PORT = "ZOOKEEPER_PORT";
    private static final String KERBEROS_PRINCIPAL = "KERBEROS_PRINCIPAL";
    private static final String KERBEROS_KEYTAB = "KERBEROS_KEYTAB";
    private static final String HBASE_CONFIGURATION_ZOOKEEPER_QUORUM = "hbase.zookeeper.quorum";
    private static final String HBASE_CONFIGURATION_ZOOKEEPER_CLIENTPORT = "hbase.zookeeper.property.clientPort";
    private static final String KRB5_CONF = "src/main/resources/conf/krb5.conf";
    private static final String JAVA_SECURITY_KRB5_CONF = "java.security.krb5.conf";
    private static final String JAVA_KERBEROS_DEBUG = "sun.security.krb5.debug";

    private static final Logger LOG = LoggerFactory.getLogger(HBaseConnector.class.getName());
    private Configuration configuration;
    private Connection connection;
    private static HBaseConnector instance;

    public static synchronized HBaseConnector getInstance() {
        if (instance != null) return instance;
        instance = new HBaseConnector();
        return instance;
    }

    private HBaseConnector() {
        this.setConfiguration(HBaseConfiguration.create());
    }

    public void setConfiguration(Configuration configuration) {
        this.configuration = configuration;
    }

    public void connect() {
        configuration.addResource("src/main/resources/conf/hbase-site.xml");

        File krb5 = new File(KRB5_CONF);
        if (krb5.exists()) {
            LOG.debug("Found krb5.conf file '{}' so performing Kerberos authentication...", KRB5_CONF);

            String zookeeperQuorum = System.getProperty(ZOOKEEPER_QUORUM);
            String zookeeperPort = System.getProperty(ZOOKEEPER_PORT);

            if (zookeeperQuorum == null) {
                logSystemPropertyNotFound(ZOOKEEPER_QUORUM);
            } else {
                logSystemPropertyFound(ZOOKEEPER_QUORUM, zookeeperQuorum);
                configuration.set(HBASE_CONFIGURATION_ZOOKEEPER_QUORUM, zookeeperQuorum);
            }

            if (zookeeperPort == null) {
                logSystemPropertyNotFound(ZOOKEEPER_PORT);
            } else {
                logSystemPropertyFound(ZOOKEEPER_PORT, zookeeperPort);
                configuration.setInt(HBASE_CONFIGURATION_ZOOKEEPER_CLIENTPORT, Integer.valueOf(zookeeperPort));
            }

            // Point to the krb5.conf file.
            System.setProperty(JAVA_SECURITY_KRB5_CONF, KRB5_CONF);

            // Enable debugging
            System.setProperty(JAVA_KERBEROS_DEBUG, String.valueOf(LOG.isDebugEnabled()));

            // Override these values by setting -DKERBEROS_PRINCIPAL and/or -DKERBEROS_KEYTAB
            String principal = System.getProperty(KERBEROS_PRINCIPAL);
            String keytabLocation = System.getProperty(KERBEROS_KEYTAB);

            if (principal == null) {
                logSystemPropertyNotFound(KERBEROS_PRINCIPAL);
            } else {
                logSystemPropertyFound(KERBEROS_PRINCIPAL, principal);
            }

            if (keytabLocation == null) {
                logSystemPropertyNotFound(KERBEROS_KEYTAB);
            } else {
                logSystemPropertyFound(KERBEROS_KEYTAB, keytabLocation);
            }

            // Login
            UserGroupInformation.setConfiguration(configuration);
            try {
                UserGroupInformation.loginUserFromKeytab(principal, keytabLocation);
                LOG.info("Kerberos authentication successful for user '{}' using keytab file '{}'", principal, keytabLocation);
            } catch (IOException e) {
                LOG.error("Kerberos authentication failed for user '{}' using keytab file '{}'", principal, keytabLocation, e);
            }
        } else {
            LOG.debug("No krb5.conf file found at '{}' so skipping Kerberos authentication.", KRB5_CONF);
        }
    }

    private void logSystemPropertyFound(String key, String value) {
        LOG.debug("System property found for '{}' with value '{}", key, value);
    }

    private void logSystemPropertyNotFound(String key) {
        LOG.warn("No system property found for '{}'", key);
    }

    public Connection getConnection() throws IOException {
        if (connection == null || connection.isClosed()) {
            try {
                connection = ConnectionFactory.createConnection(configuration);
            } catch (IOException e) {
                LOG.error("Error getting connection to HBase", e);
                throw e;
            }
        }
        return connection;
    }

    @Override
    protected void finalize() throws Throwable {
        if (connection != null) connection.close();
    }

    private void validateSchema() throws IOException {
        LOG.info("Validating schema...");
        boolean isValid = true;
        HBaseAdmin hbaseAdmin = new HBaseAdmin(configuration);
        for (TableNames tableName : TableNames.values()) {
            if (hbaseAdmin.tableExists(tableName.getTableName())) {
                LOG.info("{} table exists", tableName.getTableName().getNameAsString());
            } else {
                LOG.error("{} table does not exist!", tableName.getTableName().getNameAsString());
                isValid = false;
            }
        }
        if (isValid) {
            LOG.info("Valid schema!");
        } else {
            LOG.error("Invalid schema!");
        }
    }

    public static void main(String[] args) throws IOException {
        HBaseConnector.getInstance().connect();
        HBaseConnector.getInstance().validateSchema();
    }

}
