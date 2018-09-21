package uk.gov.ons.sbr.data.hbase.load;

import org.apache.hadoop.mapreduce.Mapper;
import uk.gov.ons.sbr.data.domain.UnitType;
import uk.gov.ons.sbr.data.hbase.load.links.UnitLinksKVMapper;

public class KVMapperFactory {

    public static Class<? extends Mapper> getKVMapper(UnitType unitType) throws ClassNotFoundException {
        switch (unitType) {
            case COMPANY_REGISTRATION:
                return CompaniesHouseDataKVMapper.class;
            case ENTERPRISE:
                return EnterpriseDataKVMapper.class;
            case LOCAL_UNIT:
                return LocalUnitDataKVMapper.class;
            case PAYE:
                return PayeDataKVMapper.class;
            case VAT:
                return VATDataKVMapper.class;
            default:
                return null;
        }
    }

    public static Class<? extends Mapper> getKVMapper(UnitType unitType, UnitType childUnit) throws ClassNotFoundException {
        return UnitLinksKVMapper.class;
    }

}
