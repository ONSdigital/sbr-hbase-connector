package uk.gov.ons.sbr.data.controller;

import org.junit.Before;
import org.junit.Test;
import uk.gov.ons.sbr.data.domain.StatisticalUnit;
import uk.gov.ons.sbr.data.domain.UnitType;
import uk.gov.ons.sbr.data.hbase.AbstractHBaseIT;

import java.time.Month;
import java.time.YearMonth;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.junit.Assert.*;

public class StatisticalUnitControllerIT extends AbstractHBaseIT {

    private static final YearMonth TEST_REFERENCE_PERIOD = YearMonth.of(2017, Month.JUNE);
    private static final String TEST_ENTERPRISE_REFERENCE_NUMBER = "123456789";
    private static final String TEST_KEY = "635663737";
    private UnitController controller;

    @Before
    public void setup() throws Exception {
        controller = new UnitController();
    }

    @Test
    public void findUnits() throws Exception {
        // Put test unit
        Map<UnitType, String> parents = new HashMap<>();
        parents.put(UnitType.ENTERPRISE, TEST_ENTERPRISE_REFERENCE_NUMBER);
        String children = "{ch: '43546583', vat: [658873556378], paye: [567P784]}";
        controller.updateUnitLinks(TEST_REFERENCE_PERIOD, TEST_KEY, UnitType.LEGAL_UNIT, parents, children);

        // Put 2nd test unit with enterprise key +1
        parents = new HashMap<>();
        parents.put(UnitType.ENTERPRISE, TEST_ENTERPRISE_REFERENCE_NUMBER);
        controller = new UnitController();
        controller.updateUnitLinks(TEST_REFERENCE_PERIOD, String.valueOf(Integer.valueOf(TEST_KEY) + 1), UnitType.LEGAL_UNIT, parents, children);

        // Find Unit
        Optional<List<StatisticalUnit>> matchingUnits = controller.findUnits(TEST_REFERENCE_PERIOD, TEST_KEY);
        assertTrue("Failure - unit should be found", matchingUnits.isPresent());
        assertEquals("Failure - 1 matching unit should be found", 1, matchingUnits.get().size());

        // Validate returned unit
        StatisticalUnit matchingStatisticalUnit = matchingUnits.get().iterator().next();
        assertEquals("Failure - invalid reference period", TEST_REFERENCE_PERIOD, matchingStatisticalUnit.getReferencePeriod());
        assertEquals("Failure - invalid unit key", TEST_KEY, matchingStatisticalUnit.getKey());
        assertEquals("Failure - invalid unit type", UnitType.LEGAL_UNIT, matchingStatisticalUnit.getType());
        assertEquals("Failure - invalid unit type", parents, matchingStatisticalUnit.getLinks().getParents());
        assertEquals("Failure - invalid children Json string", children, matchingStatisticalUnit.getLinks().getChildJsonString());

        // Put 2nd test unit for same enterprise and reference period
        parents = new HashMap<>();
        parents.put(UnitType.ENTERPRISE, TEST_ENTERPRISE_REFERENCE_NUMBER);
        parents.put(UnitType.LEGAL_UNIT, TEST_KEY);
        controller.updateUnitLinks(TEST_REFERENCE_PERIOD, TEST_KEY, UnitType.VAT, parents, (String)null);

        // Find Units
        matchingUnits = controller.findUnits(TEST_REFERENCE_PERIOD, TEST_KEY);
        assertTrue("Failure - units should be found", matchingUnits.isPresent());
        assertEquals("Failure - 2 matching unit should be found", 2, matchingUnits.get().size());
    }

    @Test
    public void findUnitsNoMatch() throws Exception {
        // Find Units (no match)
        Optional<List<StatisticalUnit>> matchingUnits = controller.findUnits(TEST_REFERENCE_PERIOD, "xxx");
        assertFalse("Failure - no units should be found", matchingUnits.isPresent());
    }

}