package uk.gov.ons.sbr.data.controller;

import org.junit.Before;
import org.junit.Test;
import uk.gov.ons.sbr.data.domain.StatisticalUnit;
import uk.gov.ons.sbr.data.domain.StatisticalUnitLinks;
import uk.gov.ons.sbr.data.domain.UnitType;
import uk.gov.ons.sbr.data.hbase.AbstractHBaseIT;

import java.time.Month;
import java.time.YearMonth;
import java.util.*;

import static org.junit.Assert.*;

public class StatisticalUnitControllerIT extends AbstractHBaseIT {

    private static final YearMonth TEST_REFERENCE_PERIOD = YearMonth.of(2017, Month.JUNE);
    private static final String TEST_ENTERPRISE_REFERENCE_NUMBER = "123456789";
    private static final String TEST_KEY = "635663737";
    private UnitController controller;
    private Map<UnitType, String>  parents;
    private Map<String, UnitType> children;

    @Before
    public void setup() throws Exception {
        controller = new UnitController();

        // Put test unit
        parents = new EnumMap<>(UnitType.class);
        parents.put(UnitType.ENTERPRISE, TEST_ENTERPRISE_REFERENCE_NUMBER);
        children = new HashMap<>();
        children.put("43546583", UnitType.COMPANY_REGISTRATION);
        children.put("658873556378", UnitType.VAT);
        children.put("567P784", UnitType.PAYE);
        controller.updateUnitLinks(TEST_REFERENCE_PERIOD, TEST_KEY, UnitType.LEGAL_UNIT, parents, children);

        // Put 2nd test unit with enterprise key +1
        parents = new EnumMap<>(UnitType.class);
        parents.put(UnitType.ENTERPRISE, TEST_ENTERPRISE_REFERENCE_NUMBER);
        controller = new UnitController();
        controller.updateUnitLinks(TEST_REFERENCE_PERIOD, String.valueOf(Integer.valueOf(TEST_KEY) + 1), UnitType.LEGAL_UNIT, parents, children);
    }

    @Test
    public void findUnits() throws Exception {

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
        assertEquals("Failure - invalid children Json string", children, matchingStatisticalUnit.getLinks().getChildren());

        // Put 2nd test unit for same enterprise and reference period
        parents = new HashMap<>();
        parents.put(UnitType.ENTERPRISE, TEST_ENTERPRISE_REFERENCE_NUMBER);
        parents.put(UnitType.LEGAL_UNIT, TEST_KEY);
        controller.updateUnitLinks(TEST_REFERENCE_PERIOD, TEST_KEY, UnitType.VAT, parents, null);

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

    @Test
    public void getLinks() throws Exception {
        // Get Unit Links
        Optional<StatisticalUnitLinks> links = controller.getUnitLinks(TEST_REFERENCE_PERIOD, TEST_KEY, UnitType.LEGAL_UNIT);
        assertTrue("Failure - links should be found", links.isPresent());
        StatisticalUnitLinks statisticalUnitLinks = links.get();
        assertEquals("Failure - invalid unit type", parents, statisticalUnitLinks.getParents());
        assertEquals("Failure - invalid children Json string", children, statisticalUnitLinks.getChildren());
    }

}