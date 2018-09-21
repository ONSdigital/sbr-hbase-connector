package uk.gov.ons.sbr.data.hbase.dao;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Matchers;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;
import uk.gov.ons.sbr.data.hbase.table.ColumnFamilies;
import uk.gov.ons.sbr.data.hbase.table.TableNames;
import uk.gov.ons.sbr.data.hbase.util.RowKeyUtils;

import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class HBaseEnterpriseDAOTest extends AbstractHBaseEnterpriseDAOTest {

    @Mock
    private Table table;
    @Mock
    private Result result;
    @Mock
    private Connection connection;
    @Captor
    private ArgumentCaptor<Put> putCaptor;

    @Before
    public void setup() throws Exception {
        super.setup();
        HBaseEnterpriseDAO dao = new HBaseEnterpriseDAO();
        dao.setConnection(connection);
        setDao(dao);
        when(connection.getTable(TableNames.ENTERPRISE.getTableName())).thenReturn(table);
        putCaptor = ArgumentCaptor.forClass(Put.class);
    }

    @Test
    public void getEnterprise() throws Exception {
        // Create mock "Get" result
        byte[] rowKey = Bytes.toBytes(RowKeyUtils.createRowKey(TEST_REFERENCE_PERIOD, TEST_ENTERPRISE_KEY));
        List<Cell> cells = new ArrayList<>();
        testEnterpriseVariables.forEach((variable, value) -> {
                    Cell cell = CellUtil.createCell(rowKey, ColumnFamilies.ENTERPRISE_DATA.getColumnFamily(), Bytes.toBytes(variable), System.currentTimeMillis(), KeyValue.Type.Put.getCode(), Bytes.toBytes(value));
                cells.add(cell);
                });
        result = Result.create(cells);
        when(table.get(Matchers.any(Get.class))).thenReturn(result);

        // Run test
        super.getEnterprise();
    }

    @Test
    public void getEnterpriseNoResult() throws Exception {
        // Create mock "Get" result
        result = new Result();
        when(table.get(Matchers.any(Get.class))).thenReturn(result);

        //Run test
        super.getEnterpriseNoResult();
    }

    @Test
    public void putEnterprise() throws Exception {
        super.putEnterprise();

        verify(table).put(putCaptor.capture());
        Put put = putCaptor.getValue();

        // Test row key
        String[] splitKey = RowKeyUtils.splitRowKey(Bytes.toString(put.getRow()));
        assertEquals("Failure - invalid reference period", testEnterprise.getReferencePeriod().format(DateTimeFormatter.ofPattern(RowKeyUtils.getReferencePeriodFormat())), splitKey[0]);
        assertEquals("Failure - invalid enterprise key", testEnterprise.getKey(), splitKey[1]);

        // Test columns
        testEnterpriseVariables.forEach((column, value) -> {
            assertTrue("Failure - no column for " + column, put.has(ColumnFamilies.ENTERPRISE_DATA.getColumnFamily(), Bytes.toBytes(column)));
            String putColumnValue = Bytes.toString(CellUtil.cloneValue((put.get(ColumnFamilies.ENTERPRISE_DATA.getColumnFamily(),
                    Bytes.toBytes(column)).get(0))));
            assertEquals("Failure - invalid " + column, value, putColumnValue);
        });

    }

}