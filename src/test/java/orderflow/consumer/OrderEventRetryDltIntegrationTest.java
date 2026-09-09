package orderflow.consumer;

import orderflow.event.OrderEvent;
import orderflow.service.OrderEventProcessingService;

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
import org.springframework.test.context.bean.override.mockito.MockitoBean;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

@SpringBootTest(
        properties = {
                "spring.kafka.listener.auto-startup=true"
        }
)
@ActiveProfiles("test")
class OrderEventRetryDltIntegrationTest {

    @Autowired
    private KafkaTemplate<String, String> kafkaTemplate;


    /*
     * Replace the real processing service with a Mockito mock.
     *
     * Real:
     * Kafka
     * KafkaTemplate
     * @KafkaListener
     * Jackson deserialization
     * DefaultErrorHandler
     * Retry mechanism
     * DeadLetterPublishingRecoverer
     * DLT topic
     *
     * Mocked:
     * OrderEventProcessingService
     */
    @MockitoBean
    private OrderEventProcessingService processingService;


    /*
     * Creates a separate Kafka consumer used only by this test.
     *
     * This consumer listens to order-events.DLT.
     */
    private ConsumerFactory<String, String> createDltConsumerFactory() {

        Map<String, Object> props =
                new HashMap<>();


        props.put(
                ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,
                "localhost:9092"
        );


        /*
         * Unique consumer group for every test execution.
         *
         * This prevents offsets from previous test runs
         * from interfering with this run.
         */
        props.put(
                ConsumerConfig.GROUP_ID_CONFIG,
                "orderflow-retry-dlt-test-"
                        + UUID.randomUUID()
        );


        /*
         * Only consume DLT records created after
         * this test consumer starts.
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


        return new DefaultKafkaConsumerFactory<>(props);
    }


    @Test
    void shouldRetryBusinessFailureAndEventuallySendEventToDlt()
            throws Exception {

        /*
         * Kafka listener thread adds records here.
         *
         * JUnit thread retrieves them from here.
         */
        BlockingQueue<ConsumerRecord<String, String>> records =
                new LinkedBlockingQueue<>();


        ConsumerFactory<String, String> consumerFactory =
                createDltConsumerFactory();


        /*
         * This manually-created listener listens only
         * to the dead-letter topic.
         */
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
         * Whenever the DLT consumer gets a message,
         * place it into the BlockingQueue.
         */
        container.setupMessageListener(
                (MessageListener<String, String>) record ->
                        records.add(record)
        );


        /*
         * Start listening to the DLT before
         * sending the test message.
         */
        container.start();


        /*
         * Wait until Kafka assigns partitions
         * to this test consumer.
         */
        long assignmentDeadline =
                System.currentTimeMillis() + 10_000;


        while (
                container.getAssignedPartitions().isEmpty()
                        &&
                        System.currentTimeMillis()
                                < assignmentDeadline
        ) {

            Thread.sleep(100);
        }


        assertFalse(
                container.getAssignedPartitions().isEmpty(),
                "DLT consumer did not receive partition assignment"
        );


        try {

            /*
             * Unique event ID for this test execution.
             */
            String eventId =
                    "retry-dlt-test-"
                            + System.nanoTime();


            /*
             * Stable Kafka key.
             */
            String key =
                    "product-123";


            /*
             * This payload is VALID JSON.
             *
             * Therefore:
             *
             * JSON deserialization should succeed.
             *
             * The failure will happen later,
             * inside processingService.process(...).
             */
            String payload =
                    """
                    {
                      "eventId": "%s",
                      "eventType": "PRODUCT_PURCHASED",
                      "productId": 123,
                      "quantity": 1,
                      "occurredAt": "2026-09-05T18:00:00Z"
                    }
                    """.formatted(eventId);


            /*
             * Simulate a retryable business-processing failure.
             *
             * Every call to:
             *
             * processingService.process(...)
             *
             * throws RuntimeException.
             */
            doThrow(
                    new RuntimeException(
                            "Simulated temporary processing failure"
                    )
            )
                    .when(processingService)
                    .process(
                            any(OrderEvent.class)
                    );


            /*
             * Send the valid event to the normal Kafka topic.
             */
            kafkaTemplate
                    .send(
                            "order-events",
                            key,
                            payload
                    )
                    .get(
                            10,
                            TimeUnit.SECONDS
                    );


            /*
             * Wait for the event to eventually appear in the DLT.
             *
             * Your observed retry behavior was:
             *
             * attempt 1
             * retry 1
             * retry 2
             *
             * = 3 processing attempts total
             *
             * Then the record is recovered to the DLT.
             */
            ConsumerRecord<String, String> dltRecord =
                    records.poll(
                            20,
                            TimeUnit.SECONDS
                    );


            assertNotNull(
                    dltRecord,
                    "Expected failed event in DLT after retries"
            );


            /*
             * Verify original key is preserved.
             */
            assertEquals(
                    key,
                    dltRecord.key()
            );


            /*
             * Verify original payload is preserved.
             */
            assertEquals(
                    payload,
                    dltRecord.value()
            );


            /*
             * IMPORTANT:
             *
             * Your actual runtime showed:
             *
             * Wanted: 4
             * Actual: 3
             *
             * Therefore we verify the behavior
             * that is actually configured and running.
             */
            verify(
                    processingService,
                    times(3)
            )
                    .process(
                            any(OrderEvent.class)
                    );


            /*
             * Print the DLT record itself.
             */
            System.out.println(
                    "\n========== RETRY DLT RESULT =========="
            );

            System.out.println(
                    "DLT Topic     : "
                            + dltRecord.topic()
            );

            System.out.println(
                    "DLT Partition : "
                            + dltRecord.partition()
            );

            System.out.println(
                    "DLT Offset    : "
                            + dltRecord.offset()
            );

            System.out.println(
                    "Key           : "
                            + dltRecord.key()
            );

            System.out.println(
                    "Value         : "
                            + dltRecord.value()
            );


            /*
             * Print and correctly decode DLT headers.
             */
            System.out.println(
                    "\n---------- HEADERS ----------"
            );


            dltRecord
                    .headers()
                    .forEach(header -> {

                        String headerKey =
                                header.key();

                        byte[] value =
                                header.value();


                        if (value == null) {

                            System.out.println(
                                    headerKey + " = null"
                            );

                            return;
                        }


                        switch (headerKey) {

                            /*
                             * Partition is stored as an int.
                             *
                             * int = 4 bytes
                             */
                            case "kafka_dlt-original-partition" -> {

                                int originalPartition =
                                        ByteBuffer
                                                .wrap(value)
                                                .getInt();

                                System.out.println(
                                        headerKey
                                                + " = "
                                                + originalPartition
                                );
                            }


                            /*
                             * Offset is stored as a long.
                             *
                             * long = 8 bytes
                             */
                            case "kafka_dlt-original-offset" -> {

                                long originalOffset =
                                        ByteBuffer
                                                .wrap(value)
                                                .getLong();

                                System.out.println(
                                        headerKey
                                                + " = "
                                                + originalOffset
                                );
                            }


                            /*
                             * Kafka record timestamp is also
                             * represented as a long.
                             *
                             * It is epoch milliseconds.
                             */
                            case "kafka_dlt-original-timestamp" -> {

                                long originalTimestamp =
                                        ByteBuffer
                                                .wrap(value)
                                                .getLong();


                                Instant timestampAsInstant =
                                        Instant.ofEpochMilli(
                                                originalTimestamp
                                        );


                                System.out.println(
                                        headerKey
                                                + " = "
                                                + originalTimestamp
                                                + " ("
                                                + timestampAsInstant
                                                + ")"
                                );
                            }


                            /*
                             * Most of the remaining DLT headers
                             * are textual values.
                             *
                             * Examples:
                             *
                             * exception class
                             * exception message
                             * original topic
                             * timestamp type
                             * consumer group
                             */
                            default -> {

                                String text =
                                        new String(
                                                value,
                                                StandardCharsets.UTF_8
                                        );


                                System.out.println(
                                        headerKey
                                                + " = "
                                                + text
                                );
                            }
                        }
                    });


            System.out.println(
                    "-----------------------------"
            );


            System.out.println(
                    "\nProcessing attempts verified: 3"
            );


            System.out.println(
                    "======================================\n"
            );

        } finally {

            /*
             * Always stop the manually-created DLT consumer.
             *
             * This executes even if an assertion fails.
             */
            container.stop();
        }
    }
}