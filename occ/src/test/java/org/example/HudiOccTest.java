package org.example;

import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hudi.client.HoodieJavaWriteClient;
import org.apache.hudi.client.WriteStatus;
import org.apache.hudi.client.common.HoodieJavaEngineContext;
import org.apache.hudi.client.transaction.lock.FileSystemBasedLockProvider;
import org.apache.hudi.client.transaction.lock.ZookeeperBasedLockProvider;
import org.apache.hudi.common.engine.EngineType;
import org.apache.hudi.common.fs.FSUtils;
import org.apache.hudi.common.model.*;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.config.*;
import org.apache.hudi.index.HoodieIndex;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.Test;
import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.stream.IntStream;


import static org.apache.hudi.common.config.LockConfiguration.ZK_CONNECT_URL_PROP_KEY;


@Test(singleThreaded = true)
public class HudiOccTest {

    private static final Logger logger =
            LoggerFactory.getLogger(HudiOccTest.class.getName());

    @Test
    void HudiTest() throws IOException{

        Configuration hadoopConf = new Configuration();
        final String tablePath = System.getProperty("user.dir") + "/tmp/hudiTest/";
        final String tableType = HoodieTableType.COPY_ON_WRITE.name();
        final String tableName = "hudiTestTable";

        Path path = new Path(tablePath);
        FileSystem fs = FSUtils.getFs(tablePath, hadoopConf);
        if (!fs.exists(path)) {

            HoodieTableMetaClient tableMetaClient =
                    HoodieTableMetaClient.withPropertyBuilder()
                            .setTableType(tableType)
                            .setTableName(tableName)
                            .setPayloadClassName(HoodieAvroPayload.class.getName())
                            .setRecordKeyFields("ph")
                            .setPartitionFields("id,name")
                            .setKeyGeneratorClassProp(HoodieWriteConfig.KEYGENERATOR_TYPE.key())
                            .initTable(hadoopConf, tablePath);
        }

        final Schema schema =
                SchemaBuilder.record("user")
                        .doc("user")
                        .namespace("example.avro")
                        .fields()
                        .name("id")
                        .doc("Unique identifier")
                        .type()
                        .intType()
                        .noDefault()
                        .name("name")
                        .doc("Nome")
                        .type()
                        .nullable()
                        .stringType()
                        .stringDefault("Unknown")
                        .name("favorite_number")
                        .doc("number")
                        .type()
                        .nullable()
                        .intType()
                        .intDefault(0)
                        .name("favorite_color")
                        .doc("color")
                        .type()
                        .nullable()
                        .stringType()
                        .stringDefault("Unknown")
                        .name("ph")
                        .doc("mobile")
                        .type()
                        .nullable()
                        .intType()
                        .noDefault()
                        .name("entryTs")
                        .doc("tiebreaker on duplicates")
                        .type()
                        .nullable()
                        .longType()
                        .noDefault()
                        .endRecord();

        // The below means we create 1000 records with keys ranging from integer values 0-99 to
        // mimic contention. i.e: Each key will be repeated 10 times.
        int recordCountToProduce = 1000;
        int range = 100;

        ConcurrentLinkedQueue<HoodieAvroRecord<HoodieAvroPayload>> q1 = new ConcurrentLinkedQueue<>();
        IntStream intStream = IntStream.range(0, recordCountToProduce);
        intStream
                .parallel()
                .forEach(
                        i -> {
                            try {
                                final GenericRecord user = new GenericData.Record(schema);
                                user.put("id", i % range);
                                user.put("name","test");
                                HoodieAvroPayload record = new HoodieAvroPayload(user, 0);
                                final HoodieAvroRecord<HoodieAvroPayload> hoodieAvroRecord =
                                        new HoodieAvroRecord<>(
                                                new HoodieKey(user.get("id").toString(), createPartitionPath(user)),
                                                record);
                                q1.add(hoodieAvroRecord);
                            } catch (Exception e) {
                                throw new RuntimeException(e);
                            }
                        });


        Properties p = new Properties();
        p.setProperty(ZK_CONNECT_URL_PROP_KEY,"localhost");

        HoodieWriteConfig cfg =
                HoodieWriteConfig.newBuilder()
                        .withEngineType(EngineType.JAVA)
                        .withPath(tablePath)
                        .withSchema(schema.toString())
                        .forTable(tableName)
                        .withIndexConfig(
                                HoodieIndexConfig.newBuilder().withIndexType(HoodieIndex.IndexType.BLOOM).build())
                        .withAutoCommit(false)
                        .withLockConfig(
                                HoodieLockConfig.newBuilder()

//                        Case 1: Uncomment below to use file system based lock provider
                                        .withLockProvider(FileSystemBasedLockProvider.class)
                                        .withFileSystemLockExpire(1)
                                        .withClientNumRetries(10000)
                                        .withClientRetryWaitTimeInMillis(10000L)
                                        .withNumRetries(10000)
                                        .withClientRetryWaitTimeInMillis(10000L)


//                        Case 2: Uncomment below to use Zookeeper based lock provider
//                        .withLockProvider(ZookeeperBasedLockProvider.class)
//                        .withZkBasePath("/test")
//                        .withZkLockKey("test_table")
//                        .withZkSessionTimeoutInMs(1000L)
//                        .withZkConnectionTimeoutInMs(1000L)
//                        .withRetryMaxWaitTimeInMillis(1000L)
//                        .withRetryWaitTimeInMillis(1000L)
//                        .withClientRetryWaitTimeInMillis(1000L)
//                        .fromProperties(p)

//                        Case 3: Uncomment below to use Hive metastore based lock provider
//                        .withLockProvider(HiveMetastoreBasedLockProvider.class)
//                        .withHiveMetastoreURIs("thrift://localhost:9083")
//                        .withHiveDatabaseName("hudi_test_db")
//                        .withHiveTableName("hudi_test_table")
//                        .withNumRetries(100)
//                        .withLockWaitTimeInMillis(60000L)

                                        .build())
                        .withWriteConcurrencyMode(WriteConcurrencyMode.OPTIMISTIC_CONCURRENCY_CONTROL)
                        .withCleanConfig(HoodieCleanConfig.newBuilder()
                                .withFailedWritesCleaningPolicy(HoodieFailedWritesCleaningPolicy.LAZY)
                                .build())
                        .build();


        BlockingQueue<HoodieJavaWriteClient> writerQueue = new LinkedBlockingQueue<>();

        // Number of writers to mock a distributed setup
        int numHudiWriteClients = 3;
        IntStream.range(0, numHudiWriteClients).forEach(i->{
            HoodieJavaWriteClient writer =
                    new HoodieJavaWriteClient<>(new HoodieJavaEngineContext(hadoopConf), cfg);
            try {
                writerQueue.put(writer);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        });


        q1.stream()
                .parallel()
                .forEach(
                        i -> {
                            logger.info("Inserting i = {}",i);
                            HoodieJavaWriteClient writer;
                            logger.info("Entering while loop");
                            try {
                                writer = writerQueue.take();
                            } catch (InterruptedException e) {
                                throw new RuntimeException(e);
                            }
                            String startCommitTime = writer.startCommit();
                            List<WriteStatus> s = writer.upsert(Collections.singletonList(i), startCommitTime);

                            writer.commit(startCommitTime, s);
                            synchronized (writerQueue){
                                try {
                                    writerQueue.put(writer);
                                } catch (InterruptedException e) {
                                    throw new RuntimeException(e);
                                }
                            }
                        }
                );


    }
    private String createPartitionPath(final GenericRecord user1) {
        return user1.get("id").toString() + "/" + user1.get("name");
    }
}
