package uk.gov.ons.sbr.data;

import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import uk.gov.ons.sbr.data.controller.EnterpriseController;
import uk.gov.ons.sbr.data.controller.UnitController;
import uk.gov.ons.sbr.data.hbase.HBaseConfig;
import uk.gov.ons.sbr.data.hbase.table.TableNames;

import java.io.IOException;

public class HBaseConnector {

    private static final Logger LOG = LoggerFactory.getLogger(HBaseConnector.class.getName());
    private HBaseConfig config;
    private EnterpriseController enterpriseController;
    private UnitController unitController;

    public HBaseConnector(HBaseConfig config) {
        this.config = config;
        this.enterpriseController = new EnterpriseController(config);
        this.unitController = new UnitController(config);
    }

    public EnterpriseController getEnterpriseController() {
        return enterpriseController;
    }

    public UnitController getUnitController() {
        return unitController;
    }

    private void validateSchema() throws IOException {
        LOG.info("Validating schema...");
        boolean isValid = true;
        HBaseAdmin hbaseAdmin = new HBaseAdmin(config.getConfig());
        for (TableNames tableName : TableNames.values()) {
            if (hbaseAdmin.tableExists(tableName.getTableName())) {
                LOG.info("{} table exists", tableName.getTableName().getNameAsString());
            }
            else {
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

    public static void main(String [] args) throws IOException {
        HBaseConnector connector;
        if (args.length == 1) {
            connector = new HBaseConnector(new HBaseConfig(args[0]));
        } else if (args.length > 1) {
            connector = new HBaseConnector(new HBaseConfig(args[0], Integer.valueOf(args[1])));
        } else {
            connector = new HBaseConnector(new HBaseConfig());
        }
        connector.validateSchema();
    }

}
