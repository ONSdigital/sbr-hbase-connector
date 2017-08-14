package uk.gov.ons.sbr.data.domain;

import io.netty.handler.codec.http.HttpResponseStatus;
import org.apache.commons.lang.StringUtils;

import java.time.YearMonth;
import java.util.EnumMap;
import java.util.HashMap;
import java.util.Map;

/**
 * Represent the parent and child hierarchy in terms of links between statistical units
 * Note that child links are represented in Json format
 */
public class UnitLinks extends AbstractPeriodEntity {

    private Map<UnitType, String> parents;
    private String childJsonString;

    public UnitLinks(YearMonth referencePeriod, String key) {
        super(referencePeriod, key);
        this.parents = new EnumMap<>(UnitType.class);
        this.childJsonString = StringUtils.EMPTY;
    }

    public String getChildJsonString() {
        return childJsonString;
    }

    public Map<UnitType, String> getParents() {
        return parents;
    }

    public void setParents(Map<UnitType, String> parents) {
        this.parents = parents;
    }
    public void putParent(UnitType type, String key){
        this.parents.put(type, key);
    }

    public void setChildJsonString(String childJsonAsString) {
        this.childJsonString = childJsonAsString;
    }

}
