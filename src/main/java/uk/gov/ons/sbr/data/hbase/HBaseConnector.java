package uk.gov.ons.sbr.data.hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.security.UserGroupInformation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import uk.gov.ons.sbr.data.hbase.table.TableNames;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;

public class HBaseConnector {

    public static final String IN_MEMORY_HBASE = "sbr.hbase.inmemory";
    private static final String ZOOKEEPER_QUORUM = "ZOOKEEPER_QUORUM";
    private static final String ZOOKEEPER_PORT = "ZOOKEEPER_PORT";
    private static final String KERBEROS_PRINCIPAL = "KERBEROS_PRINCIPAL";
    private static final String KERBEROS_KEYTAB = "KERBEROS_KEYTAB";
    private static final String KRB5_CONF = "KRB5";
    private static final String HBASE_SITE_XML = "HBASE_SITE_XML";
    private static final String HBASE_CONFIGURATION_ZOOKEEPER_QUORUM = "hbase.zookeeper.quorum";
    private static final String HBASE_CONFIGURATION_ZOOKEEPER_CLIENTPORT = "hbase.zookeeper.property.clientPort";
    private static final String JAVA_SECURITY_KRB5_CONF = "java.security.krb5.conf";
    private static final String HBASE_SECURITY_AUTHENTICATION = "hbase.security.authentication";

    private static final Logger LOG = LoggerFactory.getLogger(HBaseConnector.class.getName());
    private Configuration configuration;
    private Connection connection;
    private boolean isInMemoryHBase = false;
    private static HBaseConnector instance;

    public static synchronized HBaseConnector getInstance() throws Exception {
        if (instance != null) return instance;
        instance = new HBaseConnector();
        // If system property set run against in memory test HBase instance
        if (Boolean.valueOf(System.getProperty(IN_MEMORY_HBASE))) {
            LOG.info("'{}' is set to true so using in memory HBase database", IN_MEMORY_HBASE);
            instance.isInMemoryHBase = true;
            // In memory database does not support namespaces so set to empty string
            System.setProperty("sbr.hbase.namespace", "");
            instance.setConfiguration(InMemoryHBase.init());
        }
        return instance;
    }

    public void setConfiguration(Configuration configuration) {
        this.configuration = configuration;
    }

    public Configuration getConfiguration() {
        return configuration;
    }

    public void connect() throws IOException {
        // If in memory HBase no need to connect
        if (isInMemoryHBase) return;

        // Configure HBase
        String hbaseSite = System.getProperty(HBASE_SITE_XML);
        if (hbaseSite == null) {
            configuration = HBaseConfiguration.create();
            LOG.debug("No system property '{}' set so using default configuration", HBASE_SITE_XML);
        } else {
            File hbaseSiteFile = new File(hbaseSite);
            if (hbaseSiteFile.exists()) {
                LOG.debug("Using settings from hbase-site.xml file at '{}'", hbaseSiteFile.getPath());
                configuration = new Configuration();
                configuration.addResource(hbaseSiteFile.getPath());
            } else {
                configuration = HBaseConfiguration.create();
                LOG.warn("No hbase-site.xml file found at '{}' so using default configuration", hbaseSite);
            }
        }

        // Authentication required?
        String krb5 = System.getProperty(KRB5_CONF);
        if (krb5 == null) {
            LOG.debug("No system property '{}' set so skipping Kerberos authentication.", KRB5_CONF);
        } else {
            File krb5File = new File(krb5);
            if (krb5File.exists()) {
                LOG.debug("Found krb5.conf file '{}' so performing Kerberos authentication...", krb5File.getPath());

                configuration.set(HBASE_SECURITY_AUTHENTICATION, "kerberos");

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
                System.setProperty(JAVA_SECURITY_KRB5_CONF, krb5File.getPath());

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
                if (principal != null && keytabLocation != null) {
                    UserGroupInformation.setConfiguration(configuration);
                    try {
                        UserGroupInformation ugi = UserGroupInformation.loginUserFromKeytabAndReturnUGI(principal, keytabLocation);
                        LOG.info("Kerberos authentication successful for user '{}' using keytab file '{}'", principal, keytabLocation);
                        LOG.debug("User: '{}'", ugi.getUserName());
                        String[] groups = ugi.getGroupNames();
                        LOG.debug("Groups: ");
                        Arrays.stream(groups).forEach(group -> LOG.debug("'{}' ", group));
                    } catch (IOException e) {
                        LOG.error("Kerberos authentication failed for user '{}' using keytab file '{}'", principal, keytabLocation, e);
                        throw e;
                    }
                }
            } else {
                LOG.warn("No krb5.conf file found at '{}' so skipping Kerberos authentication.", krb5);
            }
        }
        // Initialize connection
        configuration = HBaseConfiguration.create(configuration);
        getConnection();
    }

    private void logSystemPropertyFound(String key, String value) {
        LOG.debug("System property found for '{}' with value '{}'", key, value);
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

    private void validateSchema() throws Exception {
        LOG.info("Validating schema...");
        boolean isValid = true;
        try (Admin hbaseAdmin = HBaseConnector.getInstance().getConnection().getAdmin()) {
            for (TableNames tableName : TableNames.values()) {
                if (hbaseAdmin.tableExists(tableName.getTableName())) {
                    LOG.info("{}' table exists", tableName.getTableName().getNameAsString());
                } else {
                    LOG.error("{}' table does not exist!", tableName.getTableName().getNameAsString());
                    isValid = false;
                }
            }
        }
        if (isValid) {
            LOG.info("Valid schema!");
        } else {
            LOG.error("Invalid schema!");
        }
    }

    public static void main(String[] args) throws Exception {
        HBaseConnector.getInstance().connect();
        HBaseConnector.getInstance().validateSchema();
    }

}
