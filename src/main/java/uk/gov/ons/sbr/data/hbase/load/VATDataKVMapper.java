package uk.gov.ons.sbr.data.hbase.load;

import uk.gov.ons.sbr.data.domain.UnitType;

public class VATDataKVMapper extends AbstractUnitDataKVMapper {

    @Override
    protected UnitType getUnitType() {
        return UnitType.VAT;
    }

    @Override
    protected String getHeaderString() {
        return "entref";
    }

    @Override
    protected int getRowKeyFieldPosition() {
        return 1;
    }
}
