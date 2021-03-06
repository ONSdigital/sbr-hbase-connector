package uk.gov.ons.sbr.data.domain;

import org.apache.commons.lang.StringUtils;

import java.time.YearMonth;
import java.util.EnumMap;
import java.util.HashMap;
import java.util.Map;

/**
 * Represent the parent and child hierarchy in terms of links between statistical units
 */
public class StatisticalUnitLinks extends AbstractPeriodEntity {

    private Map<UnitType, String> parents;
    private Map<String, UnitType> children;

    public StatisticalUnitLinks(YearMonth referencePeriod, String key) {
        super(referencePeriod, key);
        this.parents = new EnumMap<>(UnitType.class);
        this.children = new HashMap<>();
    }

    public Map<UnitType, String> getParents() {
        return parents;
    }

    public void setParents(Map<UnitType, String> parents) {
        this.parents = parents;
    }

    public void putParent(UnitType type, String key) {
        this.parents.put(type, key);
    }

    public Map<String, UnitType> getChildren() {
        return children;
    }

    public void setChildren(Map<String, UnitType> children) {
        this.children = children;
    }

    public void putChild(UnitType type, String key) {
        this.children.put(key, type);
    }

}
