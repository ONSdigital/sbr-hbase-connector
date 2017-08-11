package uk.gov.ons.sbr.data.hbase.load;

import uk.gov.ons.sbr.data.domain.UnitType;

public class CompaniesHouseDataKVMapper extends AbstractUnitDataKVMapper {

    @Override
    protected boolean useCsvHeaderAsColumnNames() {
        return false;
    }

    @Override
    protected UnitType getUnitType() {
        return UnitType.COMPANY_REGISTRATION;
    }

    @Override
    protected String getHeaderString() {
        return "CompanyName";
    }

    @Override
    protected int getRowKeyFieldPosition() {
        return 1;
    }
}
