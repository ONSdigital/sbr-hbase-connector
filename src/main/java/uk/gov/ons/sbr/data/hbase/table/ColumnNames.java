package uk.gov.ons.sbr.data.hbase.table;

import uk.gov.ons.sbr.data.domain.UnitType;

public class ColumnNames {

    private static final byte[][] CH_COLUMN_NAMES = {"companyname".getBytes(), "companynumber".getBytes(), "regaddress_careof".getBytes(), "regaddress_pobox".getBytes(), "regaddress_addressline1".getBytes(), "regaddress_addressline2".getBytes(), "regaddress_posttown".getBytes(), "regaddress_county".getBytes(), "regaddress_country".getBytes(), "regaddress_postcode".getBytes(), "companycategory".getBytes(), "companystatus".getBytes(), "countryoforigin".getBytes(), "dissolutiondate".getBytes(), "incorporationdate".getBytes(), "accounts_accountrefday".getBytes(), "accounts_accountrefmonth".getBytes(), "accounts_nextduedate".getBytes(), "accounts_lastmadeupdate".getBytes(), "accounts_accountcategory".getBytes(), "returns_nextduedate".getBytes(), "returns_lastmadeupdate".getBytes(), "mortgages_nummortcharges".getBytes(), "mortgages_nummortoutstanding".getBytes(), "mortgages_nummortpartsatisfied".getBytes(), "mortgages_nummortsatisfied".getBytes(), "siccode_sictext_1".getBytes(), "siccode_sictext_2".getBytes(), "siccode_sictext_3".getBytes(), "siccode_sictext_4".getBytes(), "limitedpartnerships_numgenpartners".getBytes(), "limitedpartnerships_numlimpartners".getBytes(), "uri".getBytes(), "previousname_1_condate".getBytes(), "previousname_1_companyname".getBytes(), "previousname_2_condate".getBytes(), "previousname_2_companyname".getBytes(), "previousname_3_condate".getBytes(), "previousname_3_companyname".getBytes(), "previousname_4_condate".getBytes(), "previousname_4_companyname".getBytes(), "previousname_5_condate".getBytes(), "previousname_5_companyname".getBytes(), "previousname_6_condate".getBytes(), "previousname_6_companyname".getBytes(), "previousname_7_condate".getBytes(), "previousname_7_companyname".getBytes(), "previousname_8_condate".getBytes(), "previousname_8_companyname".getBytes(), "previousname_9_condate".getBytes(), "previousname_9_companyname".getBytes(), "previousname_10_condate".getBytes(), "previousname_10_companyname".getBytes(), "confstmtnextduedate".getBytes(), "confstmtlastmadeupdate".getBytes(), "ref_period".getBytes()};

    public static byte[][] forUnitType(UnitType unitType) {
        switch (unitType) {
            case COMPANY_REGISTRATION:
                return CH_COLUMN_NAMES;
            default:
                return null;
        }
    }

}
