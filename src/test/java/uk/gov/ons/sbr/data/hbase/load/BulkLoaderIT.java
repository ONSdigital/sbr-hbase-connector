package uk.gov.ons.sbr.data.hbase.load;

import org.apache.hadoop.util.ToolRunner;
import org.junit.Test;
import uk.gov.ons.sbr.data.domain.UnitType;
import uk.gov.ons.sbr.data.hbase.AbstractHBaseIT;
import uk.gov.ons.sbr.data.hbase.HBaseConnector;

import java.io.File;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class BulkLoaderIT extends AbstractHBaseIT {

    private static final String TEST_CH_CSV = "src/test/resources/input/sbr-2500-ent-ch-data.csv";

    @Test
    public void load() throws Exception {
        File file = new File(TEST_CH_CSV);
        assertTrue("Test file not found", file.exists());

        int result = ToolRunner.run(HBaseConnector.getInstance().getConfiguration(), new BulkLoader(), new String[] {UnitType.COMPANY_REGISTRATION.toString(), "201708", TEST_CH_CSV});
        assertEquals("Bulk load failed", 0, result);

    }

}