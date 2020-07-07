package org.apache.hadoop.hbase.client;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.RequestIdPropagation.RequestIdPropagation;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.*;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

public class TestRequestIdPropagation {
    private static final Log LOG = LogFactory.getLog(TestFromClientSide3.class);
    private final static HBaseTestingUtility TEST_UTIL
            = new HBaseTestingUtility();
    private static byte[] FAMILY = Bytes.toBytes("testFamily");
    private static Random random = new Random();
    private static int SLAVES = 3;
    private static final byte [] ROW = Bytes.toBytes("testRow");
    private static final byte[] ANOTHERROW = Bytes.toBytes("anotherrow");
    private static final byte [] QUALIFIER = Bytes.toBytes("testQualifier");
    private static final byte [] VALUE = Bytes.toBytes("testValue");

    /**
     * @throws java.lang.Exception
     */
    @BeforeClass
    public static void setUpBeforeClass() throws Exception {
        TEST_UTIL.getConfiguration().setBoolean(
                "hbase.online.schema.update.enable", true);
        TEST_UTIL.startMiniCluster(SLAVES);
    }

    /**
     * @throws java.lang.Exception
     */
    @AfterClass
    public static void tearDownAfterClass() throws Exception {
        TEST_UTIL.shutdownMiniCluster();
    }

    /**
     * @throws java.lang.Exception
     */
    @Before
    public void setUp() throws Exception {
        // Nothing to do.
    }

    /**
     * @throws java.lang.Exception
     */
    @After
    public void tearDown() throws Exception {
        for (HTableDescriptor htd: TEST_UTIL.getHBaseAdmin().listTables()) {
            LOG.info("Tear down, remove table=" + htd.getTableName());
            TEST_UTIL.deleteTable(htd.getTableName());
        }
    }

    @Test
    public void testRequestIdPropagation() throws Exception {
        Table table = TEST_UTIL.createTable(TableName.valueOf("testHTableWithLargeBatch"),
                new byte[][] { FAMILY });
        int iterations = 0;
        List actions = new ArrayList();
        Put put1,put2;
            Object[] results = new Object[(iterations + 1) * 2];

            for (int i = 0; i < iterations + 1; i ++) {
                System.out.println("start");
                put1 = new Put(ROW);
                put1.addColumn(FAMILY, QUALIFIER, VALUE);
                RequestIdPropagation.assignInitialRequestId(put1);
                RequestIdPropagation.logRequestIdReached(put1);
                actions.add(put1);
                put2 = new Put(ANOTHERROW);
                put2.addColumn(FAMILY, QUALIFIER, VALUE);
                RequestIdPropagation.assignInitialRequestId(put2);
                RequestIdPropagation.logRequestIdReached(put2);
                actions.add(put2);

            }
            table.batch(actions, results);
            Scan scan = new Scan();
            scan.setId("123123");
            System.out.println("come");
            scan.withStartRow(ROW).withStopRow(ROW, true).addFamily(FAMILY);
            Result result;
            ResultScanner scanner = table.getScanner(scan);
                List<Result> list = new ArrayList<>();
                /*
                 * The first scan rpc should return a result with 2 cells, because 3MB + 4MB > 4MB; The second
                 * scan rpc should return a result with 3 cells, because reach the batch limit = 3; The
                 * mayHaveMoreCellsInRow in last result should be false in the scan rpc. BTW, the
                 * moreResultsInRegion also would be false. Finally, the client should collect all the cells
                 * into two result: 2+3 -> 3+2;
                 */
                while ((result = scanner.next()) != null) {
                    list.add(result);
                }


            table.close();

    }
}
