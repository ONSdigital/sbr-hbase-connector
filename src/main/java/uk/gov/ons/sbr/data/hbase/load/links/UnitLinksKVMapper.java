package uk.gov.ons.sbr.data.hbase.load.links;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import uk.gov.ons.sbr.data.domain.UnitType;
import uk.gov.ons.sbr.data.hbase.load.AbstractUnitDataKVMapper;
import uk.gov.ons.sbr.data.hbase.table.ColumnFamilies;
import uk.gov.ons.sbr.data.hbase.util.RowKeyUtils;

import java.io.IOException;

import static uk.gov.ons.sbr.data.hbase.dao.HBaseStatisticalUnitLinksDAO.CHILD_COLUMN_PREFIX;
import static uk.gov.ons.sbr.data.hbase.dao.HBaseStatisticalUnitLinksDAO.PARENT_COLUMN_PREFIX;

public class UnitLinksKVMapper extends AbstractUnitDataKVMapper {

    private UnitType parentUnitType;
    private UnitType childUnitType;
    private byte[] parentColumn;
    private byte[] childColumnValue;
    private static final String[] HEADER = {"entref", "UBRN"};

    @Override
    protected UnitType getUnitType() {
        return UnitType.UNKNOWN;
    }

    @Override
    protected String[] getHeaderStrings() {
        return HEADER;
    }

    @Override
    protected int getRowKeyFieldPosition() {
        return 0;
    }

    protected boolean useCsvHeaderAsColumnNames() {
        return false;
    }

    @Override
    protected byte[] getColumnFamily() {
        return ColumnFamilies.UNIT_LINKS_DATA.getColumnFamily();
    }

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        super.setup(context);
        this.parentUnitType = UnitType.fromString(context.getConfiguration().get("parent.unit.type"));
        this.childUnitType = UnitType.fromString(context.getConfiguration().get("child.unit.type"));
        this.parentColumn = (PARENT_COLUMN_PREFIX + parentUnitType.toString()).getBytes();
        this.childColumnValue = (childUnitType.toString()).getBytes();
    }

    @Override
    protected void map(LongWritable key, Text value, Context context) throws InterruptedException, IOException {
        // Skip header
        if (isHeaderRow(value)) return;

        String[] fields = parseLine(value, context);
        if (fields == null) return;

        writeParentChildRows(context, parentUnitType, fields[0], childUnitType, fields[1]);
    }

    private void writeParentChildRows(Context context, UnitType parentUnitType, String parentKey, UnitType childUnitType, String childKey) throws IOException, InterruptedException {
        // Write parent row
        String rowKeyStr = RowKeyUtils.createRowKey(getReferencePeriod(), parentKey, parentUnitType.toString());
        writeColumnValue(context, rowKeyStr, (CHILD_COLUMN_PREFIX + childKey).getBytes(), childColumnValue);

        // Write child row
        rowKeyStr = RowKeyUtils.createRowKey(getReferencePeriod(), childKey, childUnitType.toString());
        writeColumnValue(context, rowKeyStr, parentColumn, parentKey);
    }

}
