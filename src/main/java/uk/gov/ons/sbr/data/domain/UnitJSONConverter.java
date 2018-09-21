package uk.gov.ons.sbr.data.domain;

import org.apache.htrace.fasterxml.jackson.core.JsonProcessingException;
import org.apache.htrace.fasterxml.jackson.databind.ObjectMapper;
import org.apache.htrace.fasterxml.jackson.databind.SerializationFeature;


public class UnitJSONConverter {

    public static String toJson(StatisticalUnit unit) {
        String writeValueAsString = null;
        ObjectMapper mapper = new ObjectMapper();
        mapper.configure(SerializationFeature.WRITE_EMPTY_JSON_ARRAYS, false);
        try {
            writeValueAsString = mapper.writeValueAsString(unit);
        } catch (JsonProcessingException e) {
            e.printStackTrace();
        }
        return writeValueAsString;
    }
}
