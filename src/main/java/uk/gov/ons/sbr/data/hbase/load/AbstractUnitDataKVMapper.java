package uk.gov.ons.sbr.data.hbase.load;

import com.opencsv.CSVParser;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import uk.gov.ons.sbr.data.domain.UnitType;
import uk.gov.ons.sbr.data.hbase.table.ColumnFamilies;
import uk.gov.ons.sbr.data.hbase.table.ColumnNames;
import uk.gov.ons.sbr.data.hbase.util.RowKeyUtils;

import java.io.IOException;
import java.time.YearMonth;
import java.time.format.DateTimeFormatter;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import static uk.gov.ons.sbr.data.hbase.load.BulkLoader.REFERENCE_PERIOD;

/**
 * hbase org.apache.hadoop.hbase.mapreduce.ImportTsv -Dimporttsv.separator=, -Dimporttsv.mapper.class=my.Mapper
 * <p>
 * hbase jar /Users/harrih/Git/sbr-hbase-connector/target/sbr-hbase-connector-1.0-SNAPSHOT-distribution.jar
 */
public abstract class AbstractUnitDataKVMapper extends
        Mapper<LongWritable, Text, ImmutableBytesWritable, Put> {

    //CSV file header
    private static final Logger LOG = LoggerFactory.getLogger(AbstractUnitDataKVMapper.class.getName());

    private CSVParser csvParser;
    private YearMonth referencePeriod;
    private byte[][] columnNames;
    private byte[] columnFamily;

    public YearMonth getReferencePeriod() {
        return referencePeriod;
    }

    protected abstract UnitType getUnitType();

    protected abstract String[] getHeaderStrings();

    protected abstract int getRowKeyFieldPosition();

    protected boolean useCsvHeaderAsColumnNames() {
        return true;
    }

    protected byte[][] getColumnNames() {
        return ColumnNames.forUnitType(getUnitType());
    }

    protected byte[] getColumnFamily() {
        return ColumnFamilies.forUnitType(getUnitType());
    }

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        if (!useCsvHeaderAsColumnNames()) {
            LOG.debug("Using pre-defined column headers for {} file not those in the csv file", getUnitType());
            columnNames = getColumnNames();
        }
        columnFamily = getColumnFamily();
        String periodStr = System.getProperty(REFERENCE_PERIOD);
        try {
            referencePeriod = YearMonth.parse(periodStr, DateTimeFormatter.ofPattern(RowKeyUtils.getReferencePeriodFormat()));
        } catch (Exception e) {
            LOG.error("Cannot parse '{}' system property with value '{}'. Format should be '{}'", REFERENCE_PERIOD, periodStr, RowKeyUtils.getReferencePeriodFormat());
            throw e;
        }
        csvParser = new CSVParser();
    }

    @Override
    protected void map(LongWritable key, Text value, Context context) throws InterruptedException, IOException {

        // Skip header
        if (isHeaderRow(value)) return;

        String[] fields = parseLine(value, context);
        if (fields == null) return;

        // Key: e.g. "2017-06~07382019"
        String rowKeyStr = RowKeyUtils.createRowKey(referencePeriod, fields[getRowKeyFieldPosition()]);
        writeRow(context, rowKeyStr, fields);
    }

    protected boolean isHeaderRow(Text value) throws IOException {
        for (String headerString : getHeaderStrings()) {
            if (value.find(headerString) > -1) {
                if (useCsvHeaderAsColumnNames()) {
                    LOG.debug("Found csv header row: {}", value.toString());
                    LOG.debug("Using header row as table column names");
                    try {
                        String[] columnNameStrs = csvParser.parseLine(value.toString().trim());
                        List<byte[]> byteList = Arrays.stream(columnNameStrs).map(String::getBytes).collect(Collectors.toList());
                        columnNames = byteList.toArray(new byte[0][0]);
                    } catch (Exception e) {
                        LOG.error("Cannot parse column headers, error is: {}", e);
                        throw e;
                    }
                }
                return true;
            }
        }
        return false;
    }

    protected String[] parseLine(Text value, Context context) {
        try {
            return csvParser.parseLine(value.toString());
        } catch (Exception e) {
            LOG.error("Cannot parse line '{}', error is: {}", value.toString(), e.getMessage());
            context.getCounter(this.getClass().getSimpleName(), "PARSE_ERRORS").increment(1);
            return null;
        }
    }

    protected void writeRow(Context context, String rowKeyStr, String[] fields) throws IOException, InterruptedException {
        ImmutableBytesWritable rowKey = new ImmutableBytesWritable();

        rowKey.set(rowKeyStr.getBytes());
        Put put = new Put(rowKey.copyBytes());

        for (int i = 0; i < fields.length; i++) {
            if (!fields[i].isEmpty()) {
                try {
                    put.add(new KeyValue(rowKey.get(), columnFamily, columnNames[i], fields[i].getBytes()));
                } catch (Exception e) {
                    LOG.error("Cannot write line '{}'", fields[0], e);
                    throw e;
                }
            }
        }
        context.write(rowKey, put);
    }

    protected void writeColumnValue(Context context, String rowKeyStr, byte[] column, String valueStr) throws IOException, InterruptedException {
        if (!valueStr.isEmpty()) {
            writeColumnValue(context, rowKeyStr, column, valueStr.getBytes());
        }
    }

    protected void writeColumnValue(Context context, String rowKeyStr, byte[] column, byte[] value) throws IOException, InterruptedException {
        ImmutableBytesWritable rowKey = new ImmutableBytesWritable();

        rowKey.set(rowKeyStr.getBytes());
        Put put = new Put(rowKey.copyBytes());
        try {
            put.add(new KeyValue(rowKey.get(), columnFamily, column, value));
        } catch (Exception e) {
            LOG.error("Cannot write line '{}'", value, e);
            throw e;
        }

        context.write(rowKey, put);
    }

}
