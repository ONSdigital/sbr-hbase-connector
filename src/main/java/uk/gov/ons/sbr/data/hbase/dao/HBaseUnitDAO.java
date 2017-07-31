package uk.gov.ons.sbr.data.hbase.dao;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import uk.gov.ons.sbr.data.dao.UnitDAO;
import uk.gov.ons.sbr.data.domain.Unit;
import uk.gov.ons.sbr.data.domain.UnitLinks;
import uk.gov.ons.sbr.data.domain.UnitType;
import uk.gov.ons.sbr.data.hbase.HBaseConfig;
import uk.gov.ons.sbr.data.hbase.util.RowKeyUtils;

import java.io.IOException;
import java.time.YearMonth;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

public class HBaseUnitDAO implements UnitDAO {

    public static final String UNIT_LINKS_TABLE_NAME = "unit_links";
    public static final byte[] UNIT_LINKS_CF = Bytes.toBytes("d");
    private static final String CHILDREN_COLUMN = "children";
    private static final String PARENT_COLUMN_PREFIX = "p_";
    private static final Logger LOG = LoggerFactory.getLogger(HBaseUnitDAO.class.getName());
    private HBaseConfig config;

    public HBaseUnitDAO(HBaseConfig config) {
        this.config = config;
    }

    @Override
    public  Optional<List<Unit>> scanUnits(YearMonth referencePeriod, String key) throws IOException {
        LOG.debug("Searching for units for reference period '{}' and key '{}'", referencePeriod, key);
        String partialRowKey = RowKeyUtils.createRowKey(referencePeriod, key);
        Optional<List<Unit>> matchingUnits;
        try (Connection connection = config.getConnection()) {
            try (Table table = connection.getTable(TableName.valueOf(UNIT_LINKS_TABLE_NAME))) {
                byte[] prefix = Bytes.toBytes(partialRowKey);
                Scan scan = new Scan(prefix);
                scan.addFamily(UNIT_LINKS_CF);
                scan.setRowPrefixFilter(prefix);
                try (ResultScanner results = table.getScanner(scan)) {
                    List<Unit> units = new ArrayList<>();
                    results.forEach((Result result) -> {
                        String rowKey = Bytes.toString(result.getRow());
                        LOG.debug("Found unit with key '{}' matching partial row key '{}'", rowKey, partialRowKey);
                        Unit unit = RowKeyUtils.createUnitFromRowKey(rowKey);
                        unit.setLinks(convertToUnitLinks(referencePeriod, key, result));
                        units.add(unit);
                        });
                    LOG.debug("Total units found matching partial row key '{}' is {}", partialRowKey, units.size());
                    matchingUnits = Optional.of(units);
                }
            }
        }
        catch (IOException e) {
            LOG.error("Error getting enterprise data for partial row key '{}'", partialRowKey, e);
            throw e;
        }
        return matchingUnits;
    }

    @Override
    public Optional<UnitLinks> getUnitLinks(YearMonth referencePeriod, String key, UnitType type) throws IOException {
        String rowKey = RowKeyUtils.createRowKey(referencePeriod, key, type.toString());
        Optional<UnitLinks> links;
        try (Connection connection = config.getConnection()) {
            try (Table table = connection.getTable(TableName.valueOf(UNIT_LINKS_TABLE_NAME))) {
                Get get = new Get(Bytes.toBytes(rowKey));
                Result result = table.get(get);
                if (result.isEmpty()) {
                    LOG.debug("No unit links data found for row key '{}'", rowKey);
                    links = Optional.empty();
                } else {
                    LOG.debug("Found unit links data for row key '{}'", rowKey);
                    links = Optional.of(convertToUnitLinks(referencePeriod, key, result));
                }
            }
        }
        catch (IOException e) {
            LOG.error("Error getting enterprise data for row key '{}'", rowKey, e);
            throw e;
        }
        return links;
    }

    private UnitLinks convertToUnitLinks(YearMonth referencePeriod, String key, Result result) {
        UnitLinks links = new UnitLinks(referencePeriod, key);
        for (Cell cell : result.listCells()) {
            String column = new String(CellUtil.cloneQualifier(cell));
            String value = new String(CellUtil.cloneValue(cell));
            // Columns starting with 'p_' denote a parent
            if (column.startsWith(PARENT_COLUMN_PREFIX)) {
                LOG.debug("Found unit link parent {} with value '{}'", column, value);
                links.putParent(UnitType.fromString(column.substring(PARENT_COLUMN_PREFIX.length())), value);
            } else if (column.equals(CHILDREN_COLUMN)){
                LOG.debug("Found unit link children {} with value '{}'", column, value);
                links.setChildJsonString(value);
            } else {
                LOG.debug("Found unit link column {} with value '{}' - IGNORING", column, value);
            }
        }
        return links;
    }

    @Override
    public void putUnitLinks(UnitLinks links, UnitType type) throws IOException {
        Put linksRow;
        String rowKey = RowKeyUtils.createRowKey(links.getReferencePeriod(), links.getKey(), type.toString());

        try (Connection connection = config.getConnection();
             Table table = connection.getTable(TableName.valueOf(UNIT_LINKS_TABLE_NAME))) {

            linksRow = new Put(Bytes.toBytes(rowKey));
            // Add parents
            links.getParents().forEach((parentType, value)-> linksRow.addColumn(UNIT_LINKS_CF, Bytes.toBytes(PARENT_COLUMN_PREFIX + parentType.toString()), Bytes.toBytes(value)));
            // Add children
            if (!links.getChildJsonString().isEmpty()) {
                linksRow.addColumn(UNIT_LINKS_CF, Bytes.toBytes(CHILDREN_COLUMN), Bytes.toBytes(links.getChildJsonString()));
            }

            table.put(linksRow);
            LOG.debug("Inserted unit links data for row key '{}'", rowKey);
        } catch (IOException e) {
            LOG.error("Error inserting unit links data for row key '{}'", rowKey, e);
            throw e;
        }
    }
}
