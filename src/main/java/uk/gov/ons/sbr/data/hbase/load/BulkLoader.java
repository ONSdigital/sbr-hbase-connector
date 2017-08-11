package uk.gov.ons.sbr.data.hbase.load;

import com.google.common.base.Function;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import uk.gov.ons.sbr.data.domain.UnitType;
import uk.gov.ons.sbr.data.hbase.HBaseConnector;
import uk.gov.ons.sbr.data.hbase.table.TableNames;
import uk.gov.ons.sbr.data.hbase.util.RowKeyUtils;

import java.io.File;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.time.Duration;
import java.time.Instant;
import java.time.Period;
import java.time.YearMonth;
import java.time.format.DateTimeFormatter;
import java.util.Date;

/**
 * HBase bulk import example<br>
 * Data preparation MapReduce job driver
 * <ol>
 * <li>args[0]: unit type ("CH", "ENT", "VAT, "PAYE")
 * <li>args[1]: reference period, e.g. 201706
 * <li>args[2]: HDFS input path
 * <li>args[3]: HDFS output path (optional)
 * </ol>
 */
public class BulkLoader extends Configured implements Tool {

    static final String REFERENCE_PERIOD = "REFERENCE_PERIOD";
    private static final int SUCCESS = 0;
    private static final int ERROR = -1;
    private static final int MIN_ARGS = 3;
    private static final int MAX_ARGS = 4;
    private static final int ARG_LOAD_TYPE = 0;
    private static final int ARG_REFERENCE_PERIOD = 1;
    private static final int ARG_CSV_FILE = 2;
    private static final int ARG_HFILE_OUT_DIR = 3;
    private static final Logger LOG = LoggerFactory.getLogger(AbstractUnitDataKVMapper.class.getName());


    @Override
    public int run(String[] strings) throws Exception {
        if (strings.length < MIN_ARGS || strings.length > MAX_ARGS ) {
            System.out.println("INVALID ARGS, expected: load type, period, csv input file path, hfile output path (optional)");
        }
        try {
            YearMonth.parse(strings[ARG_REFERENCE_PERIOD], DateTimeFormatter.ofPattern(RowKeyUtils.getReferencePeriodFormat()));
            System.setProperty(REFERENCE_PERIOD, strings[ARG_REFERENCE_PERIOD]);
        } catch (Exception e) {
            LOG.error("Cannot parse reference period with value '{}'. Format should be '{}'", strings[ARG_REFERENCE_PERIOD], RowKeyUtils.getReferencePeriodFormat());
            System.exit(ERROR);
        }
        UnitType unitType = UnitType.fromString(strings[ARG_LOAD_TYPE]);
        if (unitType.equals(UnitType.UNKNOWN)) {
            LOG.error("Unknown unit type " + strings[ARG_LOAD_TYPE]);
            System.exit(ERROR);
        }
        if (strings.length == MIN_ARGS) {
            return (load(unitType, strings[ARG_REFERENCE_PERIOD], strings[ARG_CSV_FILE]));
        } else {
            return load(unitType, strings[ARG_REFERENCE_PERIOD], strings[ARG_CSV_FILE], strings[ARG_HFILE_OUT_DIR]);
        }
    }

    private int load(UnitType unitType, String referencePeriod, String inputFile) {
        return load(unitType, referencePeriod, inputFile, "");
    }

    private int load(UnitType unitType, String referencePeriod, String inputFile, String outputFilePath) {

        LOG.info("Starting bulk load of {} data for reference period '{}'", unitType.toString(), referencePeriod);

        // Time job
        Instant start = Instant.now();
        Connection connection;
        Job job;
        try {
            TableName tableName = TableNames.forUnitType(unitType);
            Class<? extends Mapper> mapper = KVMapperFactory.getKVMapper(unitType);
            connection = HBaseConnector.getInstance().getConnection();
            Configuration conf = this.getConf();
            job = Job.getInstance(conf, String.format("SBR %s Data Import", unitType.toString()));
            job.setJarByClass(mapper);
            job.setMapperClass(mapper);
            job.setMapOutputKeyClass(ImmutableBytesWritable.class);
            job.setMapOutputValueClass(Put.class);
            job.setInputFormatClass(TextInputFormat.class);
            FileInputFormat.setInputPaths(job, new Path(inputFile));

            // If we are writing HFiles
            if (!outputFilePath.isEmpty()) {
                try (Table table = connection.getTable(tableName)) {
                    try (RegionLocator regionLocator = connection.getRegionLocator(tableName)) {
                        {
                            job.setOutputFormatClass(HFileOutputFormat2.class);
                            job.setCombinerClass(PutCombiner.class);
                            job.setReducerClass(PutSortReducer.class);

                            // Auto configure partitioner and reducer
                            HFileOutputFormat2.configureIncrementalLoad(job, table, regionLocator);
                            FileOutputFormat.setOutputPath(job, new Path(String.format("%s%s%s_%s_%s", outputFilePath, File.pathSeparator, unitType.toString(), referencePeriod, start.toString())));

                            if (job.waitForCompletion(true)) {
                                try (Admin admin = connection.getAdmin()) {
                                    // Load generated HFiles into table
                                    LoadIncrementalHFiles loader = new LoadIncrementalHFiles(conf);
                                    loader.doBulkLoad(new Path(outputFilePath), admin, table, regionLocator);
                                }
                            } else {
                                LOG.error("Loading of data failed.");
                                return ERROR;
                            }
                        }
                    }
                }
            } else {
                TableMapReduceUtil.initTableReducerJob(tableName.getNameAsString(), (Class) null, job);
                job.setNumReduceTasks(0);
                if (!job.waitForCompletion(true)) {
                    LOG.error("Loading of data failed.");
                    return ERROR;
                }
            }


        } catch (Exception e) {
            LOG.error("Loading of data failed.", e);
            return ERROR;
        }

        Instant end = Instant.now();
        long seconds = Duration.between(start, end).getSeconds();
        LOG.info(String.format("Data loaded in %d:%02d:%02d", seconds / 3600, (seconds % 3600) / 60, (seconds % 60)));
        return SUCCESS;

    }

    public static void main(String[] args) throws Exception {
        try {
            HBaseConnector.getInstance().connect();
        } catch (IOException e) {
            LOG.error("Failed to connect to HBase", e);
            System.exit(ERROR);
        }
        int result = ToolRunner.run(HBaseConnector.getInstance().getConfiguration(), new BulkLoader(), args);
        System.exit(result);
    }
}
