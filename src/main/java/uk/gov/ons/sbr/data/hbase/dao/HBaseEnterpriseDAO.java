package uk.gov.ons.sbr.data.hbase.dao;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import uk.gov.ons.sbr.data.dao.EnterpriseDAO;
import uk.gov.ons.sbr.data.domain.Enterprise;
import uk.gov.ons.sbr.data.hbase.table.ColumnFamilies;
import uk.gov.ons.sbr.data.hbase.table.TableNames;
import uk.gov.ons.sbr.data.hbase.util.RowKeyUtils;

import java.io.IOException;
import java.time.YearMonth;
import java.util.Optional;

/**
 * HBase implementation of an Enterprise DAO
 */
public class HBaseEnterpriseDAO extends AbstractHBaseDAO implements EnterpriseDAO {

    private static final TableName ENTERPRISE_TABLE = TableNames.ENTERPRISE.getTableName();
    private static final byte[] ENTERPRISE_CF = ColumnFamilies.ENTERPRISE_DATA.getColumnFamily();
    private static final Logger LOG = LoggerFactory.getLogger(HBaseEnterpriseDAO.class.getName());

    @Override
    public Optional<Enterprise> getEnterprise(YearMonth referencePeriod, String key) throws IOException {
        return this.getEnterprise(RowKeyUtils.createRowKey(referencePeriod, key));
    }

    private Optional<Enterprise> getEnterprise(String rowKey) throws IOException {
        Optional<Enterprise> enterprise;
        try (Table table = getConnection().getTable(ENTERPRISE_TABLE)) {
            Get get = new Get(Bytes.toBytes(rowKey));
            Result result = table.get(get);
            if (result.isEmpty()) {
                LOG.debug("No enterprise data found for row key '{}'", rowKey);
                enterprise = Optional.empty();
            } else {
                LOG.debug("Found enterprise data for row key '{}'", rowKey);
                enterprise = Optional.of(convertToEnterprise(result));
            }
        } catch (IOException e) {
            LOG.error("Error getting enterprise data for row key '{}'", rowKey, e);
            throw e;
        }
        return enterprise;
    }

    private Enterprise convertToEnterprise(Result result) {
        Enterprise enterprise = RowKeyUtils.createEnterpriseFromRowKey(Bytes.toString(result.getRow()));
        for (Cell cell : result.listCells()) {
            String column = new String(CellUtil.cloneQualifier(cell));
            String value = new String(CellUtil.cloneValue(cell));
            LOG.debug("Found enterprise data column '{}' with value '{}'", column, value);
            enterprise.putVariable(column, value);
        }
        return enterprise;
    }

    @Override
    public void putEnterprise(Enterprise enterprise) throws IOException {
        Put enterpriseRow;
        String rowKey = RowKeyUtils.createRowKey(enterprise.getReferencePeriod(), enterprise.getKey());
        try (Table table = getConnection().getTable(ENTERPRISE_TABLE)) {
            enterpriseRow = new Put(Bytes.toBytes(rowKey));
            enterprise.getVariables().forEach((variable, value) -> enterpriseRow.addColumn(ENTERPRISE_CF, Bytes.toBytes(variable), Bytes.toBytes(value)));
            table.put(enterpriseRow);
            LOG.debug("Inserted enterprise data for row key '{}'", rowKey);
        } catch (IOException e) {
            LOG.error("Error inserting enterprise data for row key '{}'", rowKey, e);
            throw e;
        }
    }
}

