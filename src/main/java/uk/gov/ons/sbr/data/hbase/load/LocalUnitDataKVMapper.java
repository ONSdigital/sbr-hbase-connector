package uk.gov.ons.sbr.data.hbase.load;

import uk.gov.ons.sbr.data.domain.UnitType;

public class LocalUnitDataKVMapper extends AbstractUnitDataKVMapper {

    private static final String[] HEADER = {"louref"};

    @Override
    protected String[] getHeaderStrings() {
        return HEADER;
    }

    @Override
    protected UnitType getUnitType() {
        return UnitType.LOCAL_UNIT;
    }

    @Override
    protected int getRowKeyFieldPosition() {
        return 0;
    }
}
