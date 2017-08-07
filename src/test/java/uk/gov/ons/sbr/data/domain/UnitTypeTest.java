package uk.gov.ons.sbr.data.domain;

import org.junit.Test;

import static org.junit.Assert.*;

public class UnitTypeTest {
    @Test
    public void toStringTest() throws Exception {
        assertEquals("Failure - invalid string value for enterprise", "ENT", UnitType.ENTERPRISE.toString());
        assertEquals("Failure - invalid string value for legal unit", "LEU", UnitType.LEGAL_UNIT.toString());
        assertEquals("Failure - invalid string value for enterprise", "CH", UnitType.COMPANY_REGISTRATION.toString());
        assertEquals("Failure - invalid string value for enterprise", "PAYE", UnitType.PAYE.toString());
        assertEquals("Failure - invalid string value for enterprise", "VAT", UnitType.VAT.toString());
    }

    @Test
    public void fromString() throws Exception {
        // Test uppercase
        assertEquals("Failure - invalid string value for enterprise", UnitType.ENTERPRISE, UnitType.ENTERPRISE.fromString("ENT"));
        // Test lowercase
        assertEquals("Failure - invalid string value for enterprise", UnitType.ENTERPRISE, UnitType.ENTERPRISE.fromString("ent"));
        // Test no match
        assertEquals("Failure - UNKNOWN unit type expected", UnitType.UNKNOWN, UnitType.ENTERPRISE.fromString("XXX"));
    }

}