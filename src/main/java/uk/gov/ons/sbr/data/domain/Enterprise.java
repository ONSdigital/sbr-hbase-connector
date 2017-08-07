package uk.gov.ons.sbr.data.domain;

import java.time.YearMonth;
import java.util.HashMap;
import java.util.Map;

/**
 * Represents an Enterprise Unit
 */
public class Enterprise extends Unit {

    private Map<String, String> variables;

    public Enterprise(YearMonth referencePeriod, String key) {
        super(referencePeriod, key, UnitType.ENTERPRISE);
        this.variables = new HashMap<>();
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

}
