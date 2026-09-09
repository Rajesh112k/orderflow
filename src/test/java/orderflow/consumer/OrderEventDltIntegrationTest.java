package orderflow.consumer;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;

import org.junit.jupiter.api.Test;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.ApplicationContext;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.ConcurrentMessageListenerContainer;
import org.springframework.kafka.listener.ContainerProperties;
import org.springframework.kafka.listener.KafkaMessageListenerContainer;
import org.springframework.kafka.listener.MessageListener;
import org.springframework.kafka.listener.MessageListenerContainer;
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
                "spring.kafka.listener.auto-startup=true",
                "spring.kafka.consumer.auto-offset-reset=latest",

                /*
                 * Give THIS test's real OrderEventConsumer
                 * its own Kafka group.
                 *
                 * This prevents competition with another
                 * locally running OrderFlow application using:
                 *
                 * orderflow-consumer
                 */
                "orderflow.kafka.consumer.group-id="
                        + "orderflow-malformed-dlt-main-test"
        }
)
@ActiveProfiles("test")
class OrderEventDltIntegrationTest {


    @Autowired
    private KafkaTemplate<String, String> kafkaTemplate;


    /*
     * Used to access Spring Kafka's listener registry.
     *
     * This lets us wait for the real application's
     * order-events listener to actually receive a Kafka
     * partition before publishing the malformed record.
     */
    @Autowired
    private ApplicationContext applicationContext;


    /*
     * ============================================================
     * CREATE TEST-ONLY DLT CONSUMER
     * ============================================================
     *
     * This consumer watches:
     *
     * order-events.DLT
     *
     * It is not part of the production application.
     */
    private ConsumerFactory<String, String>
    createDltConsumerFactory() {

        Map<String, Object> props =
                new HashMap<>();


        props.put(
                ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,
                "localhost:9092"
        );


        /*
         * Unique consumer group on every execution.
         *
         * This means committed offsets from previous test runs
         * cannot affect this observer.
         */
        props.put(
                ConsumerConfig.GROUP_ID_CONFIG,
                "orderflow-dlt-test-"
                        + UUID.randomUUID()
        );


        /*
         * Start at the current end of the DLT.
         *
         * We care only about records produced during
         * this test execution.
         */
        props.put(
                ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,
                "latest"
        );


        props.put(
                ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
                StringDeserializer.class
        );


        props.put(
                ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
                StringDeserializer.class
        );


        return new DefaultKafkaConsumerFactory<>(
                props
        );
    }


    /*
     * ============================================================
     * WAIT FOR REAL ORDER-EVENTS LISTENER
     * ============================================================
     *
     * Do not use:
     *
     * Thread.sleep(2000)
     *
     * because CI may be slower than the local machine.
     *
     * Instead we wait until Spring Kafka confirms that the
     * application's real listener actually owns a partition.
     */
    private void waitForMainKafkaListenerAssignment()
            throws InterruptedException {

        KafkaListenerEndpointRegistry registry =
                applicationContext.getBean(
                        KafkaListenerEndpointRegistry.class
                );


        long deadline =
                System.currentTimeMillis()
                        + 20_000;


        while (System.currentTimeMillis() < deadline) {

            for (MessageListenerContainer listenerContainer :
                    registry.getListenerContainers()) {


                String[] topics =
                        listenerContainer
                                .getContainerProperties()
                                .getTopics();


                if (topics == null) {
                    continue;
                }


                boolean orderEventsListener =
                        false;


                for (String topic : topics) {

                    if ("order-events".equals(topic)) {

                        orderEventsListener =
                                true;

                        break;
                    }
                }


                if (!orderEventsListener) {
                    continue;
                }


                /*
                 * The production listener factory uses
                 * ConcurrentMessageListenerContainer.
                 */
                if (listenerContainer
                        instanceof ConcurrentMessageListenerContainer<?, ?>
                        concurrentContainer) {


                    int totalAssigned =
                            concurrentContainer
                                    .getContainers()
                                    .stream()
                                    .mapToInt(
                                            child ->
                                                    child
                                                            .getAssignedPartitions()
                                                            .size()
                                    )
                                    .sum();


                    /*
                     * At least one assigned partition is enough.
                     *
                     * Do not require three because your local
                     * order-events topic currently has only
                     * one partition.
                     */
                    if (totalAssigned > 0) {

                        System.out.println(
                                "\nMain order-events listener ready. "
                                        + "Assigned partitions: "
                                        + totalAssigned
                                        + "\n"
                        );

                        return;
                    }

                } else {


                    if (!listenerContainer
                            .getAssignedPartitions()
                            .isEmpty()) {

                        return;
                    }
                }
            }


            Thread.sleep(100);
        }


        fail(
                "Main order-events Kafka listener did not receive "
                        + "a partition assignment within 20 seconds"
        );
    }


    /*
     * ============================================================
     * WAIT FOR THIS TEST'S DLT RECORD
     * ============================================================
     *
     * The DLT is physically shared.
     *
     * Therefore we must NOT simply accept the first record.
     *
     * We filter specifically using this test's unique Kafka key.
     */
    private ConsumerRecord<String, String>
    waitForMatchingDltRecord(
            BlockingQueue<ConsumerRecord<String, String>> records,
            String expectedKey,
            long timeout,
            TimeUnit timeUnit
    ) throws InterruptedException {


        long deadline =
                System.currentTimeMillis()
                        + timeUnit.toMillis(timeout);


        while (System.currentTimeMillis() < deadline) {

            long remaining =
                    deadline
                            - System.currentTimeMillis();


            if (remaining <= 0) {
                break;
            }


            ConsumerRecord<String, String> candidate =
                    records.poll(
                            Math.min(
                                    remaining,
                                    500
                            ),
                            TimeUnit.MILLISECONDS
                    );


            if (candidate == null) {
                continue;
            }


            /*
             * Ignore unrelated DLT records.
             */
            if (expectedKey.equals(candidate.key())) {

                return candidate;
            }
        }


        return null;
    }


    /*
     * ============================================================
     * MALFORMED JSON -> DLT TEST
     * ============================================================
     *
     * Expected flow:
     *
     * malformed record
     *      ↓
     * order-events
     *      ↓
     * OrderEventConsumer
     *      ↓
     * ObjectMapper.readValue(...)
     *      ↓
     * JacksonException
     *      ↓
     * DefaultErrorHandler
     *      ↓
     * non-retryable exception
     *      ↓
     * DeadLetterPublishingRecoverer
     *      ↓
     * order-events.DLT
     */
    @Test
    void shouldSendMalformedJsonToDeadLetterTopic()
            throws Exception {


        BlockingQueue<ConsumerRecord<String, String>> records =
                new LinkedBlockingQueue<>();


        ConsumerFactory<String, String> consumerFactory =
                createDltConsumerFactory();


        ContainerProperties containerProperties =
                new ContainerProperties(
                        "order-events.DLT"
                );


        KafkaMessageListenerContainer<String, String> container =
                new KafkaMessageListenerContainer<>(
                        consumerFactory,
                        containerProperties
                );


        /*
         * Every DLT record observed by the test consumer
         * is placed into this queue.
         */
        container.setupMessageListener(
                (MessageListener<String, String>) records::add
        );


        /*
         * Start observing the DLT before publishing our
         * malformed record.
         */
        container.start();


        /*
         * ========================================================
         * WAIT FOR TEST DLT CONSUMER ASSIGNMENT
         * ========================================================
         */
        long assignmentDeadline =
                System.currentTimeMillis()
                        + 10_000;


        while (
                container
                        .getAssignedPartitions()
                        .isEmpty()
                        &&
                        System.currentTimeMillis()
                                < assignmentDeadline
        ) {

            Thread.sleep(100);
        }


        assertFalse(
                container
                        .getAssignedPartitions()
                        .isEmpty(),
                "DLT consumer did not receive a partition assignment"
        );


        try {

            /*
             * Unique key for THIS test execution.
             */
            String key =
                    "malformed-test-"
                            + System.nanoTime();


            /*
             * Intentionally invalid JSON.
             *
             * OrderEventConsumer expects JSON representing:
             *
             * OrderEvent
             *
             * but receives plain text instead.
             */
            String malformedPayload =
                    "PRODUCT_PURCHASED";


            /*
             * ====================================================
             * WAIT FOR REAL APPLICATION LISTENER
             * ====================================================
             *
             * This prevents the test from publishing before
             * the real @KafkaListener has joined its isolated
             * consumer group.
             */
            waitForMainKafkaListenerAssignment();


            System.out.println(
                    "\nSending malformed Kafka record..."
            );


            System.out.println(
                    "Kafka Key: "
                            + key
            );


            System.out.println(
                    "Payload  : "
                            + malformedPayload
            );


            /*
             * ====================================================
             * PUBLISH MALFORMED RECORD
             * ====================================================
             */
            var sendResult =
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


            System.out.println(
                    "Partition: "
                            + sendResult
                            .getRecordMetadata()
                            .partition()
            );


            System.out.println(
                    "Offset   : "
                            + sendResult
                            .getRecordMetadata()
                            .offset()
            );


            /*
             * ====================================================
             * FIND ONLY THIS TEST'S DLT RECORD
             * ====================================================
             *
             * Previously we did:
             *
             * records.poll(15, TimeUnit.SECONDS)
             *
             * which could accidentally return an unrelated
             * DLT record.
             *
             * Now we wait specifically for our unique key.
             */
            ConsumerRecord<String, String> dltRecord =
                    waitForMatchingDltRecord(
                            records,
                            key,
                            15,
                            TimeUnit.SECONDS
                    );


            assertNotNull(
                    dltRecord,
                    "Expected malformed record in DLT"
            );


            /*
             * Kafka key should survive DLT publishing.
             */
            assertEquals(
                    key,
                    dltRecord.key()
            );


            /*
             * Original malformed payload should also
             * be preserved.
             */
            assertEquals(
                    malformedPayload,
                    dltRecord.value()
            );


            /*
             * ====================================================
             * DISPLAY DLT RECORD
             * ====================================================
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
             * ====================================================
             * DISPLAY DLT HEADERS
             * ====================================================
             *
             * DeadLetterPublishingRecoverer attaches metadata
             * describing where the record originally came from
             * and what exception occurred.
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
                         * Display them as UTF-8 for debugging.
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


            System.out.println(
                    "Malformed JSON successfully routed to DLT.\n"
            );

        } finally {

            /*
             * Always stop the manually-created Kafka consumer.
             */
            container.stop();
        }
    }
}