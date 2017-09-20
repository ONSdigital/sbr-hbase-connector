package uk.gov.ons.sbr.data.controller;

import org.junit.Before;
import org.junit.Test;
import uk.gov.ons.sbr.data.domain.Enterprise;
import uk.gov.ons.sbr.data.hbase.AbstractHBaseIT;
import uk.gov.ons.sbr.data.hbase.util.ReferencePeriodUtils;

import java.io.IOException;
import java.time.Month;
import java.time.YearMonth;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class EnterpriseControllerIT extends AbstractHBaseIT {

    private static final YearMonth TEST_REFERENCE_PERIOD = YearMonth.of(2017, Month.JUNE);
    private static final String TEST_ENTERPRISE_REFERENCE_NUMBER = "123456789";
    private static final String TEST_UPDATED_BY= "unit test";
    private EnterpriseController controller;

    @Before
    public void setup() throws Exception {
        controller = new EnterpriseController();
    }

    @Test
    public void getEnterpriseForCurrentPeriod() throws Exception {
        // Use the update method to insert a new Enterprise
        controller.updateEnterpriseVariableValue(ReferencePeriodUtils.getCurrentPeriod(), TEST_ENTERPRISE_REFERENCE_NUMBER, TEST_UPDATED_BY,"name", "MyEnterprise");

        //Retrieve the inserted Enterprise
        Enterprise enterprise = validateReturnedEnterprise(controller.getEnterprise(TEST_ENTERPRISE_REFERENCE_NUMBER));

        assertEquals("Failure - invalid enterprise reference period", ReferencePeriodUtils.getCurrentPeriod(), enterprise.getReferencePeriod());
        assertEquals("Failure - invalid enterprise name", "MyEnterprise", enterprise.getVariables().get("name"));
    }

    @Test
    public void getEnterprise() throws Exception {
        // Use the update method to insert a new Enterprise
        controller.updateEnterpriseVariableValue(TEST_REFERENCE_PERIOD, TEST_ENTERPRISE_REFERENCE_NUMBER, TEST_UPDATED_BY, "name", "MyEnterprise");

        //Retrieve the inserted Enterprise
        Enterprise enterprise = validateReturnedEnterprise(controller.getEnterprise(TEST_ENTERPRISE_REFERENCE_NUMBER));

        assertEquals("Failure - invalid enterprise reference period", TEST_REFERENCE_PERIOD, enterprise.getReferencePeriod());
        assertEquals("Failure - invalid enterprise name", "MyEnterprise", enterprise.getVariables().get("name"));
    }

    @Test
    public void updateEnterpriseVariableValue() throws Exception {
        controller.updateEnterpriseVariableValue(TEST_REFERENCE_PERIOD, TEST_ENTERPRISE_REFERENCE_NUMBER, TEST_UPDATED_BY,"name", "MyEnterprise");
        controller.updateEnterpriseVariableValue(TEST_REFERENCE_PERIOD, TEST_ENTERPRISE_REFERENCE_NUMBER, TEST_UPDATED_BY,"name", "MyNewEnterpriseName");

        // Retrieve the updated Enterprise
        Enterprise enterprise = validateReturnedEnterprise(controller.getEnterpriseForReferencePeriod(TEST_REFERENCE_PERIOD, TEST_ENTERPRISE_REFERENCE_NUMBER));

        assertEquals("Failure - invalid enterprise reference period", TEST_REFERENCE_PERIOD, enterprise.getReferencePeriod());
        assertEquals("Failure - invalid enterprise name", "MyNewEnterpriseName", enterprise.getVariables().get("name"));
    }

    @Test
    public void updateEnterpriseVariableValues() throws Exception {
        Map<String, String> oldValues = new HashMap<>();
        oldValues.put("name", "MyEnterprise");
        oldValues.put("employees", "10");
        oldValues.put("turnover", "100000");
        controller.updateEnterpriseVariableValues(TEST_REFERENCE_PERIOD, TEST_ENTERPRISE_REFERENCE_NUMBER, TEST_UPDATED_BY, oldValues);

        Map<String, String> newValues = new HashMap<>();
        newValues.put("name", "MyNewEnterpriseName");
        newValues.put("employment", "9");
        newValues.put("turnover", "101000");
        controller.updateEnterpriseVariableValues(TEST_REFERENCE_PERIOD, TEST_ENTERPRISE_REFERENCE_NUMBER, TEST_UPDATED_BY, newValues);

        // Retrieve the updated Enterprise
        Enterprise enterprise = validateReturnedEnterprise(controller.getEnterpriseForReferencePeriod(TEST_REFERENCE_PERIOD, TEST_ENTERPRISE_REFERENCE_NUMBER));

        assertEquals("Failure - invalid enterprise reference period", TEST_REFERENCE_PERIOD, enterprise.getReferencePeriod());
        assertEquals("Failure - invalid enterprise name", "MyNewEnterpriseName", enterprise.getVariables().get("name"));
        assertEquals("Failure - invalid enterprise employees", "10", enterprise.getVariables().get("employees"));
        assertEquals("Failure - invalid enterprise turnover", "101000", enterprise.getVariables().get("turnover"));
        assertEquals("Failure - invalid enterprise employment", "9", enterprise.getVariables().get("employment"));

    }

    private Enterprise validateReturnedEnterprise(Optional<Enterprise> optEnt) throws IOException {
        assertTrue("Failure - no enterprise found", optEnt.isPresent());
        Enterprise enterprise = optEnt.get();
        assertEquals("Failure - invalid enterprise reference number", TEST_ENTERPRISE_REFERENCE_NUMBER, enterprise.getKey());
        return enterprise;
    }

}