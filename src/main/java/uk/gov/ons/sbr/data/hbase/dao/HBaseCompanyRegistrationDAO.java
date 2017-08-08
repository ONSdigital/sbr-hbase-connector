package uk.gov.ons.sbr.data.hbase.dao;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import uk.gov.ons.sbr.data.dao.CompanyRegistrationDAO;
import uk.gov.ons.sbr.data.dao.EnterpriseDAO;
import uk.gov.ons.sbr.data.domain.CompanyRegistration;
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
public class HBaseCompanyRegistrationDAO extends AbstractHBaseDAO implements CompanyRegistrationDAO {

    private static final TableName COMPANY_REGISTRATION_TABLE = TableNames.COMPANIES_HOUSE_DATA.getTableName();
    private static final byte[] COMAPNY_DATA_CF = ColumnFamilies.COMPANY_DATA.getColumnFamily();
    private static final Logger LOG = LoggerFactory.getLogger(HBaseCompanyRegistrationDAO.class.getName());

    @Override
    public Optional<CompanyRegistration> getCompanyRegistration(YearMonth referencePeriod, String key) throws IOException {
        return this.getCompanyRegistration(RowKeyUtils.createRowKey(referencePeriod, key));
    }

    private Optional<CompanyRegistration> getCompanyRegistration(String rowKey) throws IOException {
        Optional<CompanyRegistration> enterprise;
        try (Table table = getConnection().getTable(COMPANY_REGISTRATION_TABLE)) {
            Get get = new Get(Bytes.toBytes(rowKey));
            Result result = table.get(get);
            if (result.isEmpty()) {
                LOG.debug("No companies house data found for row key '{}'", rowKey);
                enterprise = Optional.empty();
            } else {
                LOG.debug("Found companies house data for row key '{}'", rowKey);
                enterprise = Optional.of(convertToCompanyRegistration(result));
            }
        } catch (IOException e) {
            LOG.error("Error getting companies house data for row key '{}'", rowKey, e);
            throw e;
        }
        return enterprise;
    }

    private CompanyRegistration convertToCompanyRegistration(Result result) {
        CompanyRegistration companyRegistration = RowKeyUtils.createCompanyRegistrationFromRowKey(Bytes.toString(result.getRow()));
        for (Cell cell : result.listCells()) {
            String column = new String(CellUtil.cloneQualifier(cell));
            String value = new String(CellUtil.cloneValue(cell));
            LOG.debug("Found companies house data column '{}' with value '{}'", column, value);
            companyRegistration.putVariable(column, value);
        }
        return companyRegistration;
    }

}

