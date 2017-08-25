package uk.gov.ons.sbr.data.hbase.dao;

import org.junit.Before;
import org.junit.Test;
import uk.gov.ons.sbr.data.domain.Enterprise;

import java.time.Month;
import java.time.YearMonth;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

import static org.junit.Assert.*;

public abstract class AbstractHBaseEnterpriseDAOTest {

    static final YearMonth TEST_REFERENCE_PERIOD = YearMonth.of(2017, Month.JUNE);
    static final String TEST_ENTERPRISE_KEY = "123456789";
    private static final String TEST_INVALID_ENTERPRISE_KEY = "000000000";
    private HBaseEnterpriseDAO dao;
    Map<String, String> testEnterpriseVariables;
    Enterprise testEnterprise;

    void setDao(HBaseEnterpriseDAO dao) {
        this.dao = dao;
    }

    @Before
    public void setup() throws Exception {
        testEnterprise = new Enterprise(TEST_REFERENCE_PERIOD, TEST_ENTERPRISE_KEY);
        testEnterpriseVariables = new HashMap<>();
        testEnterpriseVariables.put("name", "MyEnterprise");
        testEnterpriseVariables.put("employees", "10");
        testEnterpriseVariables.put("turnover", "100000");
        testEnterprise.putVariables(testEnterpriseVariables);
    }

    @Test
    public void getEnterprise() throws Exception {
        Optional<Enterprise> optEnterprise = dao.getEnterprise(TEST_REFERENCE_PERIOD, TEST_ENTERPRISE_KEY);

        // Test we have a result
        assertTrue("Failure - result should not be empty", optEnterprise.isPresent());
        Enterprise enterprise = optEnterprise.get();

        // Test enterprise period and key
        assertEquals("Failure - invalid reference period", TEST_REFERENCE_PERIOD, enterprise.getReferencePeriod());
        assertEquals("Failure - invalid enterprise key", TEST_ENTERPRISE_KEY, enterprise.getKey());

        // Test enterprise variables
        Map enterpriseVariables = enterprise.getVariables();
        testEnterpriseVariables.forEach((variable, value) -> {
            assertTrue("Failure - no variable for " + variable, enterpriseVariables.containsKey(variable));
            assertEquals("Failure - invalid " + variable, value, enterpriseVariables.get(variable));
        });
    }

    @Test
    public void getEnterpriseNoResult() throws Exception {
        Optional<Enterprise> optEnterprise = dao.getEnterprise(TEST_REFERENCE_PERIOD, TEST_INVALID_ENTERPRISE_KEY);
        assertFalse("Failure - result should be empty", optEnterprise.isPresent());
    }

    @Test
    public void putEnterprise() throws Exception {
        dao.putEnterprise(testEnterprise);
    }

}