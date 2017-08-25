package uk.gov.ons.sbr.data.hbase.load;

import org.apache.hadoop.util.ToolRunner;
import org.junit.Test;
import uk.gov.ons.sbr.data.controller.AdminDataController;
import uk.gov.ons.sbr.data.controller.EnterpriseController;
import uk.gov.ons.sbr.data.controller.LocalUnitController;
import uk.gov.ons.sbr.data.controller.UnitController;
import uk.gov.ons.sbr.data.domain.*;
import uk.gov.ons.sbr.data.hbase.AbstractHBaseIT;
import uk.gov.ons.sbr.data.hbase.HBaseConnector;
import uk.gov.ons.sbr.data.hbase.util.RowKeyUtils;

import java.io.File;
import java.time.Month;
import java.time.YearMonth;
import java.time.format.DateTimeFormatter;
import java.util.List;
import java.util.Optional;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static uk.gov.ons.sbr.data.hbase.load.BulkLoader.UNIT_SEPARATOR;

public class BulkLoaderIT extends AbstractHBaseIT {

    private static final YearMonth TEST_PERIOD = YearMonth.of(2017, Month.JUNE);
    private static final String TEST_PERIOD_STR = TEST_PERIOD.format(DateTimeFormatter.ofPattern(RowKeyUtils.getReferencePeriodFormat()));
    private static final String TEST_CH_CSV = "src/test/resources/input/sbr-2500-ent-ch-data.csv";
    private static final String TEST_PAYE_CSV = "src/test/resources/input/paye-data.csv";
    private static final String TEST_VAT_CSV = "src/test/resources/input/vat-data.csv";
    private static final String TEST_ENT_CSV = "src/test/resources/input/enterprise-data.csv";
    private static final String TEST_LOCAL_UNIT_CSV = "src/test/resources/input/local-unit-data.csv";
    private static final String TEST_ENT_LEU_LINKS_CSV = "src/test/resources/input/ent-leu-links.csv";
    private static final String TEST_ENT_LOU_LINKS_CSV = "src/test/resources/input/ent-lou-links.csv";
    private static final String TEST_LEU_VAT_LINKS_CSV = "src/test/resources/input/leu-vat-links.csv";
    private BulkLoader bulkLoader = new BulkLoader();
    private AdminDataController adminDataController = new AdminDataController();
    private UnitController unitController = new UnitController();
    private EnterpriseController enterpriseController = new EnterpriseController();
    private LocalUnitController localUnitController = new LocalUnitController();

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


    @Test
    public void loadEntLeuLinksData() throws Exception {
        File file = new File(TEST_ENT_LEU_LINKS_CSV);
        assertTrue("Test file not found", file.exists());

        int result = loadData(new String[]{UnitType.ENTERPRISE.toString() + UNIT_SEPARATOR + UnitType.LEGAL_UNIT.toString(), TEST_PERIOD_STR, TEST_ENT_LEU_LINKS_CSV});
        assertEquals("Bulk load failed", 0, result);

        Optional<List<StatisticalUnit>> unit = unitController.findUnits("9900000005");
        assertTrue("No unit record found", unit.isPresent());

        assertEquals("No record found", "9900000005", unit.get().get(0).getKey());
        assertEquals("No record found", "100000000011", unit.get().get(0).getLinks().getChildren().keySet().iterator().next());
        assertEquals("No record found", UnitType.LEGAL_UNIT, unit.get().get(0).getLinks().getChildren().values().iterator().next());
    }

    @Test
    public void loadLeuVatLinksData() throws Exception {
        File file = new File(TEST_LEU_VAT_LINKS_CSV);
        assertTrue("Test file not found", file.exists());

        int result = loadData(new String[]{UnitType.LEGAL_UNIT.toString() + UNIT_SEPARATOR + UnitType.VAT.toString(), TEST_PERIOD_STR, TEST_LEU_VAT_LINKS_CSV});
        assertEquals("Bulk load failed", 0, result);

        Optional<List<StatisticalUnit>> unit = unitController.findUnits("551392773603");
        assertTrue("No unit record found", unit.isPresent());

        assertEquals("No record found", "551392773603", unit.get().get(0).getKey());
    }

    @Test
    public void loadEntLouLinksData() throws Exception {
        File file = new File(TEST_ENT_LOU_LINKS_CSV);
        assertTrue("Test file not found", file.exists());

        int result = loadData(new String[]{UnitType.ENTERPRISE.toString() + UNIT_SEPARATOR + UnitType.LOCAL_UNIT.toString(), TEST_PERIOD_STR, TEST_ENT_LOU_LINKS_CSV});
        assertEquals("Bulk load failed", 0, result);

        Optional<List<StatisticalUnit>> unit = unitController.findUnits("200000001");
        assertTrue("No unit record found", unit.isPresent());

        assertEquals("No record found", "200000001", unit.get().get(0).getKey());
    }

    @Test
    public void loadLocalUnitData() throws Exception {
        File file = new File(TEST_LOCAL_UNIT_CSV);
        assertTrue("Test file not found", file.exists());

        int result = loadData(new String[]{UnitType.LOCAL_UNIT.toString(), TEST_PERIOD_STR, TEST_LOCAL_UNIT_CSV});
        assertEquals("Bulk load failed", 0, result);

        Optional<LocalUnit> localUnit = localUnitController.getLocalUnitForReferencePeriod(TEST_PERIOD, "200000001");
        assertTrue("No enterprise record found", localUnit.isPresent());

        assertEquals("No local unit record found", "200000001", localUnit.get().getKey());
    }

    @Test
    public void loadEnterpriseData() throws Exception {
        File file = new File(TEST_ENT_CSV);
        assertTrue("Test file not found", file.exists());

        int result = loadData(new String[]{UnitType.ENTERPRISE.toString() + UNIT_SEPARATOR + UnitType.LEGAL_UNIT.toString(), TEST_PERIOD_STR, TEST_ENT_LEU_LINKS_CSV});
        assertEquals("Bulk load failed", 0, result);
        result = loadData(new String[]{UnitType.ENTERPRISE.toString() + UNIT_SEPARATOR + UnitType.LOCAL_UNIT.toString(), TEST_PERIOD_STR, TEST_ENT_LOU_LINKS_CSV});
        assertEquals("Bulk load failed", 0, result);
        result = loadData(new String[]{UnitType.LEGAL_UNIT.toString() + UNIT_SEPARATOR + UnitType.VAT.toString(), TEST_PERIOD_STR, TEST_LEU_VAT_LINKS_CSV});
        assertEquals("Bulk load failed", 0, result);
        result = loadData(new String[]{UnitType.ENTERPRISE.toString(), TEST_PERIOD_STR, TEST_ENT_CSV});
        assertEquals("Bulk load failed", 0, result);

        Optional<Enterprise> enterprise = enterpriseController.getEnterpriseForReferencePeriod(TEST_PERIOD, "9900000001");
        assertTrue("No enterprise record found", enterprise.isPresent());

        assertEquals("No enterprise record found", "9900000001", enterprise.get().getKey());

        assertEquals("{\"type\":\"ENT\",\"id\":\"9900000001\",\"children\":[{\"type\":\"LOU\",\"id\":\"200000002\"},{\"type\":\"LEU\",\"id\":\"100000000003\",\"children\":[{\"type\":\"VAT\",\"id\":\"976132369059\"}]},{\"type\":\"LOU\",\"id\":\"200000003\"},{\"type\":\"LEU\",\"id\":\"100000000002\",\"children\":[{\"type\":\"VAT\",\"id\":\"346942023239\"}]},{\"type\":\"LEU\",\"id\":\"100000000001\",\"children\":[{\"type\":\"VAT\",\"id\":\"397585634298\"}]},{\"type\":\"LOU\",\"id\":\"200000001\"}]}", enterprise.get().toUnitHierarchyAsJson());
    }

    private int loadData(String[] args) throws Exception {
        return ToolRunner.run(HBaseConnector.getInstance().getConfiguration(), bulkLoader, args);
    }

}