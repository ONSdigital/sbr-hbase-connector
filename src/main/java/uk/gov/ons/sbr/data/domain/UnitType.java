package uk.gov.ons.sbr.data.domain;

/**
 * Unit types
 */
public enum UnitType {

    COMPANY_REGISTRATION("CH"),
    ENTERPRISE("ENT"),
    LEGAL_UNIT("LEU"),
    PAYE("PAYE"),
    UNKNOWN("UNKNOWN"),
    VAT("VAT");

    private final String value;

    UnitType(final String value) {
        this.value = value;
    }

    @Override
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
}
