package orderflow.consumer;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.jupiter.api.Test;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.ContainerProperties;
import org.springframework.kafka.listener.KafkaMessageListenerContainer;
import org.springframework.kafka.listener.MessageListener;
import org.springframework.test.context.ActiveProfiles;

import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;

@SpringBootTest(
        properties = {
                "spring.kafka.listener.auto-startup=true"
        }
)
@ActiveProfiles("test")
class OrderEventDltIntegrationTest {

    @Autowired
    private KafkaTemplate<String, String> kafkaTemplate;


    /*
     * Creates a Kafka consumer that is used ONLY by this test.
     *
     * This consumer listens to order-events.DLT so that the test
     * can verify that a malformed message actually reaches the DLT.
     */
    private ConsumerFactory<String, String> createDltConsumerFactory() {

        Map<String, Object> props =
                new HashMap<>();


        /*
         * Tell this test consumer where Kafka is running.
         */
        props.put(
                ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,
                "localhost:9092"
        );


        /*
         * Every test run gets a unique consumer group.
         *
         * Example:
         *
         * orderflow-dlt-test-550e8400-e29b-41d4-a716-446655440000
         *
         * This prevents committed offsets from previous test runs
         * from interfering with this test.
         */
        props.put(
                ConsumerConfig.GROUP_ID_CONFIG,
                "orderflow-dlt-test-" + UUID.randomUUID()
        );


        /*
         * Start consuming from the latest position.
         *
         * We only care about DLT messages created by THIS test,
         * not old DLT records from previous runs.
         */
        props.put(
                ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,
                "latest"
        );


        /*
         * Kafka stores keys as bytes.
         *
         * Convert those bytes back into Java Strings.
         */
        props.put(
                ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
                StringDeserializer.class
        );


        /*
         * Convert Kafka value bytes back into Java Strings.
         */
        props.put(
                ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
                StringDeserializer.class
        );


        /*
         * Build the ConsumerFactory using these properties.
         */
        return new DefaultKafkaConsumerFactory<>(props);
    }


    @Test
    void shouldSendMalformedJsonToDeadLetterTopic()
            throws Exception {

        /*
         * Thread-safe queue.
         *
         * Kafka consumer thread:
         *
         *      receives DLT record
         *              ↓
         *      records.add(record)
         *
         * JUnit thread:
         *
         *      records.poll(...)
         *              ↓
         *      retrieves that record
         */
        BlockingQueue<ConsumerRecord<String, String>> records =
                new LinkedBlockingQueue<>();


        /*
         * Create our test-only Kafka consumer factory.
         */
        ConsumerFactory<String, String> consumerFactory =
                createDltConsumerFactory();


        /*
         * Tell the listener container which topic to consume.
         */
        ContainerProperties containerProperties =
                new ContainerProperties(
                        "order-events.DLT"
                );


        /*
         * Create a Kafka listener container manually.
         *
         * This is NOT your production @KafkaListener.
         *
         * Its only purpose is to observe the DLT during this test.
         */
        KafkaMessageListenerContainer<String, String> container =
                new KafkaMessageListenerContainer<>(
                        consumerFactory,
                        containerProperties
                );


        /*
         * Whenever this test consumer receives a DLT record,
         * put that record into our BlockingQueue.
         */
        container.setupMessageListener(
                (MessageListener<String, String>) record ->
                        records.add(record)
        );


        /*
         * Start the DLT consumer.
         */
        container.start();


        /*
         * container.start() does not guarantee that Kafka has
         * already assigned partitions to the consumer.
         *
         * Therefore wait until partition assignment happens.
         */
        long assignmentDeadline =
                System.currentTimeMillis() + 10_000;


        while (
                container.getAssignedPartitions().isEmpty()
                        &&
                        System.currentTimeMillis() < assignmentDeadline
        ) {

            Thread.sleep(100);
        }


        /*
         * If this fails, the test DLT consumer never became ready.
         */
        assertFalse(
                container.getAssignedPartitions().isEmpty(),
                "DLT consumer did not receive a partition assignment"
        );


        try {

            /*
             * Generate a unique key for this test run.
             */
            String key =
                    "malformed-test-" + System.nanoTime();


            /*
             * This is intentionally NOT valid JSON.
             *
             * Our OrderEventConsumer expects something like:
             *
             * {
             *   "eventId": "...",
             *   "eventType": "PRODUCT_PURCHASED",
             *   "productId": 123
             * }
             *
             * Instead we deliberately send plain text.
             */
            String malformedPayload =
                    "PRODUCT_PURCHASED";


            /*
             * Send the malformed record to the NORMAL topic.
             *
             * Flow:
             *
             * order-events
             *      ↓
             * real OrderEventConsumer
             *      ↓
             * ObjectMapper.readValue(...)
             *      ↓
             * JacksonException
             *      ↓
             * DefaultErrorHandler
             *      ↓
             * DeadLetterPublishingRecoverer
             *      ↓
             * order-events.DLT
             */
            kafkaTemplate
                    .send(
                            "order-events",
                            key,
                            malformedPayload
                    )
                    .get(
                            10,
                            TimeUnit.SECONDS
                    );


            /*
             * Wait for our DLT consumer to receive the failed record.
             *
             * Maximum wait = 15 seconds.
             */
            ConsumerRecord<String, String> dltRecord =
                    records.poll(
                            15,
                            TimeUnit.SECONDS
                    );


            /*
             * If null, nothing reached the DLT within 15 seconds.
             */
            assertNotNull(
                    dltRecord,
                    "Expected malformed record in DLT"
            );


            /*
             * Verify that the Kafka key survived the trip to the DLT.
             */
            assertEquals(
                    key,
                    dltRecord.key()
            );


            /*
             * Verify that the original malformed payload was preserved.
             */
            assertEquals(
                    malformedPayload,
                    dltRecord.value()
            );


            /*
             * Print useful Kafka metadata.
             *
             * Remember:
             *
             * topic + partition + offset
             *
             * identifies the physical Kafka record.
             */
            System.out.println(
                    "\n========== DLT RECORD =========="
            );

            System.out.println(
                    "Topic     : "
                            + dltRecord.topic()
            );

            System.out.println(
                    "Partition : "
                            + dltRecord.partition()
            );

            System.out.println(
                    "Offset    : "
                            + dltRecord.offset()
            );

            System.out.println(
                    "Key       : "
                            + dltRecord.key()
            );

            System.out.println(
                    "Value     : "
                            + dltRecord.value()
            );


            /*
             * Print all DLT headers.
             *
             * Spring Kafka's DeadLetterPublishingRecoverer can attach
             * diagnostic information about the original failure.
             */
            System.out.println(
                    "\n========== DLT HEADERS =========="
            );


            dltRecord
                    .headers()
                    .forEach(header -> {

                        /*
                         * Kafka header values are byte[].
                         *
                         * For inspection we attempt to display them
                         * as UTF-8 text.
                         *
                         * Note:
                         * Not every possible Kafka header is necessarily
                         * textual, so this is primarily for debugging.
                         */
                        String headerValue =
                                new String(
                                        header.value(),
                                        StandardCharsets.UTF_8
                                );


                        System.out.println(
                                header.key()
                                        + " = "
                                        + headerValue
                        );
                    });


            System.out.println(
                    "================================\n"
            );

        } finally {

            /*
             * Always stop the manually-created Kafka consumer.
             *
             * Even if an assertion fails, this block executes.
             */
            container.stop();
        }
    }
}