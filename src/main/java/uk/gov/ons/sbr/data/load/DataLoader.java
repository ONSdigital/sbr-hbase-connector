package uk.gov.ons.sbr.data.load;

import org.apache.hadoop.conf.Configuration;
import uk.gov.ons.sbr.data.controller.EnterpriseController;
import uk.gov.ons.sbr.data.hbase.HBaseConfig;

import java.io.IOException;
import java.time.Month;
import java.time.YearMonth;

public class DataLoader {

    public static void main(String [] args) throws IOException {
        HBaseConfig config = new HBaseConfig();
        EnterpriseController entController =  new EnterpriseController(config);
        entController.updateEnterpriseVariableValue(YearMonth.of(2017, Month.JULY), "12345",  "name", "test");
    }
}

