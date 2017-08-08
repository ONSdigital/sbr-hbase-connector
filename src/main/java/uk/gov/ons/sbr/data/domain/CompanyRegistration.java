package uk.gov.ons.sbr.data.domain;

import java.time.YearMonth;
import java.util.HashMap;
import java.util.Map;

/**
 * Represents an Enterprise Unit
 */
public class CompanyRegistration extends Unit {

    private Map<String, String> variables;

    public CompanyRegistration(YearMonth referencePeriod, String key) {
        super(referencePeriod, key, UnitType.COMPANY_REGISTRATION);
        this.variables = new HashMap<>();
    }

    public Map<String, String> getVariables() {
        return variables;
    }

    public void putVariable(String variable, String value) {
        this.variables.put(variable, value);
    }

}
