package uk.gov.ons.sbr.data.hbase.load;

import org.apache.hadoop.mapreduce.Mapper;
import uk.gov.ons.sbr.data.domain.UnitType;

public class KVMapperFactory {

    public static Class<? extends Mapper> getKVMapper(UnitType unitType) throws ClassNotFoundException {
        switch (unitType) {
            case COMPANY_REGISTRATION:
                return CompaniesHouseDataKVMapper.class;
            default:
                return null;
        }
    }

}
