package uk.gov.ons.sbr.data.hbase.load;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.HFileOutputFormat2;
import org.apache.hadoop.hbase.mapreduce.LoadIncrementalHFiles;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import uk.gov.ons.sbr.data.domain.UnitType;
import uk.gov.ons.sbr.data.hbase.HBaseConnector;
import uk.gov.ons.sbr.data.hbase.table.TableNames;

import java.io.IOException;
import java.time.Duration;
import java.time.Instant;


/**
 * HBase bulk import example<br>
 * Data preparation MapReduce job driver
 * <ol>
 * <li>args[0]: HDFS input path
 * <li>args[1]: HDFS output path
 * <li>args[2]: unit type ("CH", "ENT", "VAT, "PAYE")
 * </ol>
 */
public class BulkLoader {
    public static void main(String[] args) {
        // Time job
        Instant start = Instant.now();
        Connection connection;
        Job job;
        try {
            UnitType unitType = UnitType.fromString(args[2]);
            if (unitType.equals(UnitType.UNKNOWN)) {
                System.out.println("Unknown unit type " + args[2]);
                System.exit(1);
            }
            TableName tableName = TableNames.forUnitType(unitType);
            Class<? extends Mapper> mapper = KVMapperFactory.getKVMapper(unitType);
            HBaseConnector.getInstance().connect();
            connection = HBaseConnector.getInstance().getConnection();
            Configuration conf = HBaseConnector.getInstance().getConfiguration();
            job = Job.getInstance(conf, "SBR Unit Data Import");
            job.setJarByClass(mapper);
            job.setMapperClass(mapper);
            job.setMapOutputKeyClass(ImmutableBytesWritable.class);
            job.setMapOutputValueClass(KeyValue.class);
            job.setInputFormatClass(TextInputFormat.class);
            job.setOutputFormatClass(HFileOutputFormat2.class);

            try (Table table = connection.getTable(tableName)) {
                RegionLocator regionLocator = connection.getRegionLocator(tableName);
                // Auto configure partitioner and reducer
                HFileOutputFormat2.configureIncrementalLoad(job, table, regionLocator);

                FileInputFormat.addInputPath(job, new Path(args[0]));
                FileOutputFormat.setOutputPath(job, new Path(args[1]));

                if (job.waitForCompletion(true)) {
                    try (Admin admin = connection.getAdmin()) {
                        // Load generated HFiles into table
                        LoadIncrementalHFiles loader = new LoadIncrementalHFiles(conf);
                        loader.doBulkLoad(new Path(args[1]), admin, table, regionLocator);
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                } else {
                    System.out.println("Loading of data failed.");
                    System.exit(1);
                }
            }
        } catch (IOException | InterruptedException | ClassNotFoundException e) {
            e.printStackTrace();
            System.exit(1);
        }

        Instant end = Instant.now();
        long seconds = Duration.between(start, end).getSeconds();
        System.out.println(String.format("Data loaded in %d:%02d:%02d", seconds / 3600, (seconds % 3600) / 60, (seconds % 60)));
    }
}