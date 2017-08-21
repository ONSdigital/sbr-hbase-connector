package uk.gov.ons.sbr.data.hbase.load;

import uk.gov.ons.sbr.data.domain.UnitType;

public class CompaniesHouseDataKVMapper extends AbstractUnitDataKVMapper {

    private static final String[] HEADER = {"CompanyName"};

    @Override
    protected String[] getHeaderStrings() {
        return HEADER;
    }

    @Override
    protected boolean useCsvHeaderAsColumnNames() {
        return false;
    }

    @Override
    protected UnitType getUnitType() {
        return UnitType.COMPANY_REGISTRATION;
    }

    @Override
    protected int getRowKeyFieldPosition() {
        return 1;
    }
}
