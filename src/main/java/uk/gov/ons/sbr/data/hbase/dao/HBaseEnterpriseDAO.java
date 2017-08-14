package uk.gov.ons.sbr.data.hbase.dao;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import uk.gov.ons.sbr.data.dao.EnterpriseDAO;
import uk.gov.ons.sbr.data.domain.CompanyRegistration;
import uk.gov.ons.sbr.data.domain.Enterprise;
import uk.gov.ons.sbr.data.domain.UnitType;
import uk.gov.ons.sbr.data.hbase.table.ColumnFamilies;
import uk.gov.ons.sbr.data.hbase.table.TableNames;
import uk.gov.ons.sbr.data.hbase.util.RowKeyUtils;

import java.io.IOException;
import java.time.YearMonth;
import java.util.Optional;

/**
 * HBase implementation of an Enterprise DAO
 */
public class HBaseEnterpriseDAO extends HBaseStatisticalUnitDAO implements EnterpriseDAO {

    private static final byte[] ENTERPRISE_CF = ColumnFamilies.ENTERPRISE_DATA.getColumnFamily();
    private static final Logger LOG = LoggerFactory.getLogger(HBaseEnterpriseDAO.class.getName());

    @Override
    public Optional<Enterprise> getEnterprise(YearMonth referencePeriod, String key) throws IOException {
        return getUnit(UnitType.ENTERPRISE, referencePeriod, key);
    }

    @Override
    public void putEnterprise(Enterprise enterprise) throws IOException {
        putUnit(enterprise);
    }
}

