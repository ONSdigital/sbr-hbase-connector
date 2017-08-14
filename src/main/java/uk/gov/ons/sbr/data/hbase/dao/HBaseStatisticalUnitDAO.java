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
import uk.gov.ons.sbr.data.dao.AdminDataDAO;
import uk.gov.ons.sbr.data.domain.*;
import uk.gov.ons.sbr.data.hbase.table.ColumnFamilies;
import uk.gov.ons.sbr.data.hbase.table.TableNames;
import uk.gov.ons.sbr.data.hbase.util.RowKeyUtils;

import java.io.IOException;
import java.time.YearMonth;
import java.util.Optional;

/**
 * HBase implementation of an Statistical Unit DAO
 */
public class HBaseStatisticalUnitDAO extends AbstractHBaseDAO {

    private static final Logger LOG = LoggerFactory.getLogger(HBaseStatisticalUnitDAO.class.getName());

    <T extends StatisticalUnit> Optional<T> getUnit(UnitType unitType, YearMonth referencePeriod, String key) throws IOException {
        return this.getUnit(unitType, RowKeyUtils.createRowKey(referencePeriod, key));
    }

    private <T extends StatisticalUnit> Optional<T> getUnit(UnitType unitType, String rowKey) throws IOException {
        Optional<T> unit;
        try (Table table = getConnection().getTable(TableNames.forUnitType(unitType))) {
            Get get = new Get(Bytes.toBytes(rowKey));
            Result result = table.get(get);
            if (result.isEmpty()) {
                LOG.debug("No {} data found for row key '{}'", unitType, rowKey);
                unit = Optional.empty();
            } else {
                LOG.debug("Found {} data for row key '{}'", unitType, rowKey);
                unit = Optional.of(convertToStatisticalUnit(unitType, result));
            }
        } catch (IOException e) {
            LOG.error("Error getting {} data for row key '{}'", unitType, rowKey, e);
            throw e;
        }
        return unit;
    }

    @SuppressWarnings("unchecked")
    private <T extends StatisticalUnit> T convertToStatisticalUnit(UnitType unitType, Result result) {
        StatisticalUnit unit = RowKeyUtils.createUnitFromRowKey(Bytes.toString(result.getRow()), unitType);
        for (Cell cell : result.listCells()) {
            String column = new String(CellUtil.cloneQualifier(cell));
            String value = new String(CellUtil.cloneValue(cell));
            LOG.debug("Found {} data column '{}' with value '{}'", unitType, column, value);
            unit.putVariable(column, value);
        }
        return (T)unit;
    }

    void putUnit(StatisticalUnit unit) throws IOException {
        Put unitRow;
        String rowKey = RowKeyUtils.createRowKey(unit.getReferencePeriod(), unit.getKey());
        try (Table table = getConnection().getTable(TableNames.forUnitType(unit.getType()))) {
            unitRow = new Put(Bytes.toBytes(rowKey));
            unit.getVariables().forEach((variable, value) -> unitRow.addColumn(ColumnFamilies.forUnitType(unit.getType()), Bytes.toBytes(variable), Bytes.toBytes(value)));
            table.put(unitRow);
            LOG.debug("Inserted {} data for row key '{}'", unit.getType(), rowKey);
        } catch (IOException e) {
            LOG.error("Error inserting {} data for row key '{}'", unit.getType(), rowKey, e);
            throw e;
        }
    }

}

