package uk.gov.ons.sbr.data.hbase.load;

import uk.gov.ons.sbr.data.domain.UnitType;

public class EnterpriseDataKVMapper extends AbstractUnitDataKVMapper {

    @Override
    protected UnitType getUnitType() {
        return UnitType.ENTERPRISE;
    }

    @Override
    protected String getHeaderString() {
        return "entref";
    }

    @Override
    protected int getRowKeyFieldPosition() {
        return 0;
    }
}
