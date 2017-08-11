package uk.gov.ons.sbr.data.hbase.load;

import com.opencsv.CSVParser;
import org.apache.hadoop.hbase.Cell;
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
import java.time.Month;
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

    protected abstract UnitType getUnitType();

    protected abstract String getHeaderString();

    protected abstract int getRowKeyFieldPosition();

    protected boolean useCsvHeaderAsColumnNames() {
        return true;
    }

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        if (!useCsvHeaderAsColumnNames()) {
            columnNames = ColumnNames.forUnitType(getUnitType());
            LOG.debug("Using pre-defined column headers for {} file not those in the csv file", getUnitType());
        }
        columnFamily = ColumnFamilies.forUnitType(getUnitType());
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
        if (value.find(getHeaderString()) > -1) {
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
            // Skip header
            return;
        }

        String[] fields;
        try {
            fields = csvParser.parseLine(value.toString());
        } catch (Exception e) {
            LOG.error("Cannot parse line '{}', error is: {}", value.toString(), e.getMessage());
            context.getCounter(this.getClass().getSimpleName(), "PARSE_ERRORS").increment(1);
            return;
        }

        ImmutableBytesWritable rowKey = new ImmutableBytesWritable();
        KeyValue kv;

        // Key: e.g. "2017-06~07382019"
        rowKey.set(RowKeyUtils.createRowKey(referencePeriod, fields[getRowKeyFieldPosition()])
                .getBytes());
        Put put = new Put(rowKey.copyBytes());

        for (int i = 0; i < fields.length; i++) {
            if (!fields[i].isEmpty()) {
                try {
                    kv = new KeyValue(rowKey.get(), columnFamily, columnNames[i], fields[i].getBytes());
                    put.add(kv);
                } catch (Exception e) {
                    LOG.error("Cannot write line '{}'", value.toString(), e);
                    throw e;
                }
            }
        }
        context.write(rowKey, put);
    }
}
