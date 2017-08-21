package uk.gov.ons.sbr.data.hbase.load;

import uk.gov.ons.sbr.data.domain.UnitType;

public class EnterpriseDataKVMapper extends AbstractUnitDataKVMapper {

    private static final String[] HEADER = {"entref"};

    @Override
    protected String[] getHeaderStrings() {
        return HEADER;
    }

    @Override
    protected UnitType getUnitType() {
        return UnitType.ENTERPRISE;
    }

    @Override
    protected int getRowKeyFieldPosition() {
        return 0;
    }
}
