package uk.gov.ons.sbr.data.domain;

import java.time.YearMonth;

/**
 * Returns Instance of a Statistical unit base on UnitType
 */
public class StatisticalUnitFactory {

    public static StatisticalUnit getUnit(UnitType unitType, YearMonth referencePeriod, String key) {
        StatisticalUnit statisticalUnit;
        switch (unitType) {
            case COMPANY_REGISTRATION:
                statisticalUnit = new CompanyRegistration(referencePeriod, key);
                break;
            case ENTERPRISE:
                statisticalUnit = new Enterprise(referencePeriod, key);
                break;
            case PAYE:
                statisticalUnit = new PAYEReturn(referencePeriod, key);
                break;
            case VAT:
                statisticalUnit = new VATReturn(referencePeriod, key);
                break;
            default:
                statisticalUnit = new StatisticalUnit(referencePeriod, key, unitType);
                break;
        }
        return statisticalUnit;
    }
}