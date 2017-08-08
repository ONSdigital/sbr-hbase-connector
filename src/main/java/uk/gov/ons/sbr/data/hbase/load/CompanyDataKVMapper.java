package uk.gov.ons.sbr.data.hbase.load;

import com.opencsv.CSVParser;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import uk.gov.ons.sbr.data.hbase.table.ColumnFamilies;
import uk.gov.ons.sbr.data.hbase.util.RowKeyUtils;

import java.io.IOException;
import java.time.YearMonth;
import java.time.format.DateTimeFormatter;

/**
 *  hbase org.apache.hadoop.hbase.mapreduce.ImportTsv -Dimporttsv.separator=, -Dimporttsv.mapper.class=my.Mapper
 *
 *  hbase jar /Users/harrih/Git/sbr-hbase-connector/target/sbr-hbase-connector-1.0-SNAPSHOT-distribution.jar
 */
public class CompanyDataKVMapper extends
        Mapper<LongWritable, Text, ImmutableBytesWritable, KeyValue> {

    //CSV file header
    private static final String[] FILE_HEADER_MAPPING = {"CompanyName", "CompanyNumber", "RegAddress.CareOf", "RegAddress.POBox", "RegAddress.AddressLine1", "RegAddress.AddressLine2", "RegAddress.PostTown", "RegAddress.County", "RegAddress.Country", "RegAddress.PostCode", "CompanyCategory", "CompanyStatus", "CountryOfOrigin", "DissolutionDate", "IncorporationDate", "Accounts.AccountRefDay", "Accounts.AccountRefMonth", "Accounts.NextDueDate", "Accounts.LastMadeUpDate", "Accounts.AccountCategory", "Returns.NextDueDate", "Returns.LastMadeUpDate", "Mortgages.NumMortCharges", "Mortgages.NumMortOutstanding", "Mortgages.NumMortPartSatisfied", "Mortgages.NumMortSatisfied", "SICCode.SicText_1", "SICCode.SicText_2", "SICCode.SicText_3", "SICCode.SicText_4", "LimitedPartnerships.NumGenPartners", "LimitedPartnerships.NumLimPartners", "URI", "PreviousName_1.CONDATE", "PreviousName_1.CompanyName", "PreviousName_2.CONDATE", "PreviousName_2.CompanyName", "PreviousName_3.CONDATE", "PreviousName_3.CompanyName", "PreviousName_4.CONDATE", "PreviousName_4.CompanyName", "PreviousName_5.CONDATE", "PreviousName_5.CompanyName", "PreviousName_6.CONDATE", "PreviousName_6.CompanyName", "PreviousName_7.CONDATE", "PreviousName_7.CompanyName", "PreviousName_8.CONDATE", "PreviousName_8.CompanyName", "PreviousName_9.CONDATE", "PreviousName_9.CompanyName", "PreviousName_10.CONDATE", "PreviousName_10.CompanyName", "ConfStmtNextDueDate", "ConfStmtLastMadeUpDate"};
    private static final String[] COLUMN_NAMES = {"companyname", "companynumber", "regaddress_careof", "regaddress_pobox", "regaddress_addressline1", "regaddress_addressline2", "regaddress_posttown", "regaddress_county", "regaddress_country", "regaddress_postcode", "companycategory", "companystatus", "countryoforigin", "dissolutiondate", "incorporationdate", "accounts_accountrefday", "accounts_accountrefmonth", "accounts_nextduedate", "accounts_lastmadeupdate", "accounts_accountcategory", "returns_nextduedate", "returns_lastmadeupdate", "mortgages_nummortcharges", "mortgages_nummortoutstanding", "mortgages_nummortpartsatisfied", "mortgages_nummortsatisfied", "siccode_sictext_1", "siccode_sictext_2", "siccode_sictext_3", "siccode_sictext_4", "limitedpartnerships_numgenpartners", "limitedpartnerships_numlimpartners", "uri", "previousname_1_condate", "previousname_1_companyname", "previousname_2_condate", "previousname_2_companyname", "previousname_3_condate", "previousname_3_companyname", "previousname_4_condate", "previousname_4_companyname", "previousname_5_condate", "previousname_5_companyname", "previousname_6_condate", "previousname_6_companyname", "previousname_7_condate", "previousname_7_companyname", "previousname_8_condate", "previousname_8_companyname", "previousname_9_condate", "previousname_9_companyname", "previousname_10_condate", "previousname_10_companyname", "confstmtnextduedate", "confstmtlastmadeupdate", "ref_period"};
    private static final String REFERENCE_PERIOD = "REFERENCE_PERIOD";
    private static final Logger LOG = LoggerFactory.getLogger(CompanyDataKVMapper.class.getName());

    private CSVParser csvParser;
    private YearMonth referencePeriod;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
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
    protected void map(LongWritable key, Text value, Context context) throws InterruptedException {

        if (value.find("CompanyName") > -1) {
            // Skip header
            return;
        }

        String[] fields;
        try {
            fields = csvParser.parseLine(value.toString());
        } catch (IOException e) {
            LOG.error("Cannot parse line '{}'", value.toString(), e);
            context.getCounter("HBaseKVMapper", "PARSE_ERRORS").increment(1);
            return;
        }

        ImmutableBytesWritable rowKey = new ImmutableBytesWritable();
        KeyValue kv;

        // Key: e.g. "2017-06~07382019"
        rowKey.set(String.format(RowKeyUtils.createRowKey(referencePeriod, fields[1]))
                .getBytes());

        for (int i = 0; i < fields.length; i++) {
            kv = new KeyValue(rowKey.get(), ColumnFamilies.COMPANY_DATA.getColumnFamily(), COLUMN_NAMES[i].getBytes(), fields[i].getBytes());
            try {
                context.write(rowKey, kv);
            } catch (IOException e) {
                LOG.error("Cannot write line '{}'", value.toString(), e);
                return;
            } catch (InterruptedException e) {
                LOG.error("Cannot write line '{}'", value.toString(), e);
                throw e;
            }
        }


    }

}
