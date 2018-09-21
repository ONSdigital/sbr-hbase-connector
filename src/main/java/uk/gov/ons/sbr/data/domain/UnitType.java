package uk.gov.ons.sbr.data.domain;

import org.apache.htrace.fasterxml.jackson.annotation.JsonValue;

import java.util.Collections;
import java.util.List;

import static java.util.Arrays.asList;

/**
 * Unit types
 */
public enum UnitType {

    COMPANY_REGISTRATION("CH", Collections.emptyList()),
    PAYE("PAYE", Collections.emptyList()),
    UNKNOWN("UNKNOWN", Collections.emptyList()),
    VAT("VAT", Collections.emptyList()),
    LEGAL_UNIT("LEU", asList(UnitType.COMPANY_REGISTRATION, UnitType.PAYE, UnitType.VAT)),
    LOCAL_UNIT("LOU", Collections.emptyList()),
    ENTERPRISE("ENT", asList(UnitType.LEGAL_UNIT, UnitType.LOCAL_UNIT));

    private final String value;
    private final List<UnitType> directDescendants;

    UnitType(final String value, final List<UnitType> directDescendants) {
        this.value = value;
        this.directDescendants = directDescendants;
    }

    @Override
    @JsonValue
    public String toString() {
        return value;
    }

    public static UnitType fromString(String text) {
        for (UnitType type : UnitType.values()) {
            if (type.value.equalsIgnoreCase(text)) {
                return type;
            }
        }
        return UNKNOWN;
    }

    public List<UnitType> getDirectDescendants() {
        return directDescendants;
    }
}
