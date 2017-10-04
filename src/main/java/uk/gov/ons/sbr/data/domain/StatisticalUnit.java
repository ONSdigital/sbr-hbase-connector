package uk.gov.ons.sbr.data.domain;

import org.apache.htrace.fasterxml.jackson.annotation.JsonIgnore;
import org.apache.htrace.fasterxml.jackson.annotation.JsonProperty;

import java.time.YearMonth;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Represents a definition of an statistical unit for a reference period i.e. a year and month
 * Each unit is of a defined type and may or may not contain parent and child links to other units
 *
 * @see UnitType
 */
public class StatisticalUnit extends AbstractPeriodEntity {

    private UnitType type;
    @JsonIgnore
    private Map<String, String> variables;
    @JsonIgnore
    private StatisticalUnitLinks links;
    @JsonProperty("children")
    private List<StatisticalUnit> children;

    public StatisticalUnit(YearMonth referencePeriod, String key, UnitType type) {
        super(referencePeriod, key);
        this.type = type;
        this.variables = new HashMap<>();
        this.links = new StatisticalUnitLinks(referencePeriod, key);
        this.children = new ArrayList<>();
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

    public StatisticalUnitLinks getLinks() {
        return links;
    }

    public void setLinks(StatisticalUnitLinks links) {
        this.links = links;
    }

    public List<StatisticalUnit> getChildren() {
        return children;
    }

    public void addChild(StatisticalUnit newChild) {
        children.add(newChild);
    }

    public String toUnitHierarchyAsJson() {
        return UnitJSONConverter.toJson(this);
    }
}
