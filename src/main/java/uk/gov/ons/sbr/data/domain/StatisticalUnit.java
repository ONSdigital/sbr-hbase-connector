package uk.gov.ons.sbr.data.domain;

import java.time.YearMonth;
import java.util.HashMap;
import java.util.Map;

/**
 * Represents a definition of an statistical unit for a reference period i.e. a year and month
 * Each unit is of a defined type and may or may not contain parent and child links to other units
 *
 * @see UnitType
 */
public class StatisticalUnit extends AbstractPeriodEntity {

    private UnitType type;
    private Map<String, String> variables;
    private UnitLinks links;

    public StatisticalUnit(YearMonth referencePeriod, String key, UnitType type) {
        super(referencePeriod, key);
        this.type = type;
        this.variables = new HashMap<>();
        this.links = new UnitLinks(referencePeriod, key);
    }

    public UnitType getType() {
        return type;
    }

    public Map<String, String> getVariables() {
        return variables;
    }

    public void putVariable(String variable, String value) {
        this.variables.put(variable, value);
    }

    public void putVariables(Map<String, String> newVariables) {
        this.variables.putAll(newVariables);
    }

    public UnitLinks getLinks() {
        return links;
    }

    public void setLinks(UnitLinks links) {
        this.links = links;
    }
}
