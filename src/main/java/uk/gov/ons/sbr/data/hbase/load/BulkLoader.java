package uk.gov.ons.sbr.data.hbase.load;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.HFileOutputFormat2;
import org.apache.hadoop.hbase.mapreduce.LoadIncrementalHFiles;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import uk.gov.ons.sbr.data.hbase.HBaseConnector;
import uk.gov.ons.sbr.data.hbase.table.TableNames;

import java.io.IOException;


/**
 * HBase bulk import example<br>
 * Data preparation MapReduce job driver
 * <ol>
 * <li>args[0]: HDFS input path
 * <li>args[1]: HDFS output path
 * <li>args[2]: HBase table name
 * </ol>
 */
public class BulkLoader {
    public static void main(String[] args) {
        Connection connection = null;
        Job job = null;
        try {
            HBaseConnector.getInstance().connect();
            connection = HBaseConnector.getInstance().getConnection();
            Configuration conf = HBaseConnector.getInstance().getConfiguration();
            job = Job.getInstance(conf, "Companies House Bulk Import Example");
            job.setJarByClass(CompanyDataKVMapper.class);
            job.setMapperClass(CompanyDataKVMapper.class);
            job.setMapOutputKeyClass(ImmutableBytesWritable.class);
            job.setMapOutputValueClass(KeyValue.class);
            job.setInputFormatClass(TextInputFormat.class);
            job.setOutputFormatClass(HFileOutputFormat2.class);


            try (Table table = connection.getTable(TableNames.COMPANIES_HOUSE_DATA.getTableName())) {
                RegionLocator regionLocator = connection.getRegionLocator(TableNames.COMPANIES_HOUSE_DATA.getTableName());
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
                    System.out.println("Loading of companies house data failed.");
                    System.exit(1);
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
            System.exit(1);
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }
    }
}