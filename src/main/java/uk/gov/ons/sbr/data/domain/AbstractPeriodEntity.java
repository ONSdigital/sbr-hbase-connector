package uk.gov.ons.sbr.data.domain;

import org.apache.htrace.fasterxml.jackson.annotation.JsonIgnore;
import org.apache.htrace.fasterxml.jackson.annotation.JsonProperty;

import java.time.YearMonth;

/**
 * Represents a definition of an entity for a reference period i.e. a year and month
 */
public abstract class AbstractPeriodEntity {

    @JsonIgnore
    private YearMonth referencePeriod;
    @JsonProperty("id")
    private String key;

    public AbstractPeriodEntity (YearMonth referencePeriod, String key) {
        this.referencePeriod = referencePeriod;
        this.key = key;
    }

    public YearMonth getReferencePeriod() {
        return referencePeriod;
    }

    public String getKey() {
        return key;
    }
}
