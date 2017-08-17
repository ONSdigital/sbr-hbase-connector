package uk.gov.ons.sbr.data.hbase.dao;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import uk.gov.ons.sbr.data.dao.AdminDataDAO;
import uk.gov.ons.sbr.data.domain.*;
import uk.gov.ons.sbr.data.hbase.table.ColumnFamilies;
import uk.gov.ons.sbr.data.hbase.table.TableNames;
import uk.gov.ons.sbr.data.hbase.util.RowKeyUtils;

import java.io.IOException;
import java.time.YearMonth;
import java.util.Optional;

/**
 * HBase implementation of an Admin Data DAO
 */
public class HBaseAdminDataDAO extends HBaseStatisticalUnitDAO implements AdminDataDAO {

    @Override
    public Optional<CompanyRegistration> getCompanyRegistration(YearMonth referencePeriod, String key) throws Exception {
        return getUnit(UnitType.COMPANY_REGISTRATION, referencePeriod, key);
    }

    @Override
    public Optional<VATReturn> getVATReturn(YearMonth referencePeriod, String key) throws Exception {
        return getUnit(UnitType.VAT, referencePeriod, key);
    }

    @Override
    public Optional<PAYEReturn> getPAYEReturn(YearMonth referencePeriod, String key) throws Exception {
        return getUnit(UnitType.PAYE, referencePeriod, key);
    }

}

