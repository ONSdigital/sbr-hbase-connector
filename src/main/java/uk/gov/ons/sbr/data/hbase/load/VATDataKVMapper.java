package uk.gov.ons.sbr.data.hbase.load;

import uk.gov.ons.sbr.data.domain.UnitType;

public class VATDataKVMapper extends AbstractUnitDataKVMapper {

    private static final String[] HEADER = {"entref"};

    @Override
    protected String[] getHeaderStrings() {
        return HEADER;
    }

    @Override
    protected UnitType getUnitType() {
        return UnitType.VAT;
    }

    @Override
    protected int getRowKeyFieldPosition() {
        return 1;
    }
}
