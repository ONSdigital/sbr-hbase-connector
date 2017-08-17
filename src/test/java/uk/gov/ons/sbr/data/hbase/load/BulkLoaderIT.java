package uk.gov.ons.sbr.data.hbase.load;

import org.apache.hadoop.util.ToolRunner;
import org.junit.Test;
import uk.gov.ons.sbr.data.controller.AdminDataController;
import uk.gov.ons.sbr.data.domain.CompanyRegistration;
import uk.gov.ons.sbr.data.domain.PAYEReturn;
import uk.gov.ons.sbr.data.domain.UnitType;
import uk.gov.ons.sbr.data.domain.VATReturn;
import uk.gov.ons.sbr.data.hbase.AbstractHBaseIT;
import uk.gov.ons.sbr.data.hbase.HBaseConnector;
import uk.gov.ons.sbr.data.hbase.InMemoryHBase;
import uk.gov.ons.sbr.data.hbase.util.RowKeyUtils;

import java.io.File;
import java.time.Month;
import java.time.YearMonth;
import java.time.format.DateTimeFormatter;
import java.util.Optional;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class BulkLoaderIT extends AbstractHBaseIT {

    private static final YearMonth TEST_PERIOD = YearMonth.of(2017, Month.JUNE);
    private static final String TEST_PERIOD_STR = TEST_PERIOD.format(DateTimeFormatter.ofPattern(RowKeyUtils.getReferencePeriodFormat()));
    private static final String TEST_CH_CSV = "src/test/resources/input/sbr-2500-ent-ch-data.csv";
    private static final String TEST_PAYE_CSV = "src/test/resources/input/paye-data.csv";
    private static final String TEST_VAT_CSV = "src/test/resources/input/vat-data.csv";
    private BulkLoader bulkLoader = new BulkLoader();
    private AdminDataController adminDataController = new AdminDataController();


    @Test
    public void loadCompaniesData() throws Exception {
        File file = new File(TEST_CH_CSV);
        assertTrue("Test file not found", file.exists());

        int result = loadData(new String[]{UnitType.COMPANY_REGISTRATION.toString(), TEST_PERIOD_STR, TEST_CH_CSV});
        assertEquals("Bulk load failed", 0, result);

        Optional<CompanyRegistration> company = adminDataController.getCompanyRegistrationForReferencePeriod(TEST_PERIOD, "04375380");
        assertTrue("No company registration found", company.isPresent());

        assertEquals("No company registration found", "04375380", company.get().getKey());
    }

    @Test
    public void loadPAYEData() throws Exception {
        File file = new File(TEST_PAYE_CSV);
        assertTrue("Test file not found", file.exists());

        int result = loadData(new String[]{UnitType.PAYE.toString(), TEST_PERIOD_STR, TEST_PAYE_CSV});
        assertEquals("Bulk load failed", 0, result);

        Optional<PAYEReturn> payeReturn = adminDataController.getPAYEReturnForReferencePeriod(TEST_PERIOD, "8878574");
        assertTrue("No PAYE record found", payeReturn.isPresent());

        assertEquals("No PAYE record found", "8878574", payeReturn.get().getKey());
    }

    @Test
    public void loadVATData() throws Exception {
        File file = new File(TEST_VAT_CSV);
        assertTrue("Test file not found", file.exists());

        int result = loadData(new String[]{UnitType.VAT.toString(), TEST_PERIOD_STR, TEST_VAT_CSV});
        assertEquals("Bulk load failed", 0, result);

        Optional<VATReturn> vatReturn = adminDataController.getVATReturnForReferencePeriod(TEST_PERIOD, "808281648666");
        assertTrue("No VAT record found", vatReturn.isPresent());

        assertEquals("No VAT record found", "808281648666", vatReturn.get().getKey());
    }

    public int loadData (String[] args) throws Exception {
        return ToolRunner.run(HBaseConnector.getInstance().getConfiguration(), bulkLoader, args);
    }

}