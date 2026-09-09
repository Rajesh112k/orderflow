package orderflow.consumer;

import orderflow.event.OrderEvent;
import orderflow.service.OrderEventProcessingService;

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
import org.springframework.test.context.bean.override.mockito.MockitoBean;

import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;


@SpringBootTest(
        properties = {
                "spring.kafka.listener.auto-startup=true",
                "spring.kafka.consumer.auto-offset-reset=latest",
                "orderflow.kafka.consumer.group-id=orderflow-poison-test-consumer"
        }
)
@ActiveProfiles("test")
class OrderEventPoisonMessageIntegrationTest {

    @Autowired
    private KafkaTemplate<String, String> kafkaTemplate;

    @Autowired
    private ApplicationContext applicationContext;

    @MockitoBean
    private OrderEventProcessingService processingService;


    private ConsumerFactory<String, String> createDltConsumerFactory() {

        Map<String, Object> props =
                new HashMap<>();

        props.put(
                ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,
                "localhost:9092"
        );

        props.put(
                ConsumerConfig.GROUP_ID_CONFIG,
                "orderflow-poison-dlt-"
                        + UUID.randomUUID()
        );

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


    /*
     * Wait until the application's REAL order-events
     * listener actually owns a Kafka partition.
     *
     * This replaces Thread.sleep(2000).
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
                        orderEventsListener = true;
                        break;
                    }
                }

                if (!orderEventsListener) {
                    continue;
                }

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

                    if (totalAssigned > 0) {

                        System.out.println(
                                "\nMain order-events listener ready. "
                                        + "Assigned partitions: "
                                        + totalAssigned
                                        + "\n"
                        );

                        return;
                    }

                } else if (!listenerContainer
                        .getAssignedPartitions()
                        .isEmpty()) {

                    return;
                }
            }

            Thread.sleep(100);
        }

        fail(
                "Main order-events Kafka listener did not receive "
                        + "a partition assignment within 20 seconds"
        );
    }


    @Test
    void shouldContinueProcessingAfterPoisonMessageIsSentToDlt()
            throws Exception {

        BlockingQueue<ConsumerRecord<String, String>> dltRecords =
                new LinkedBlockingQueue<>();

        ConsumerFactory<String, String> consumerFactory =
                createDltConsumerFactory();

        ContainerProperties containerProperties =
                new ContainerProperties(
                        "order-events.DLT"
                );

        KafkaMessageListenerContainer<String, String> dltContainer =
                new KafkaMessageListenerContainer<>(
                        consumerFactory,
                        containerProperties
                );

        dltContainer.setupMessageListener(
                (MessageListener<String, String>) dltRecords::add
        );

        dltContainer.start();


        /*
         * Wait until the test-only DLT observer
         * actually owns a DLT partition.
         */
        long assignmentDeadline =
                System.currentTimeMillis()
                        + 10_000;

        while (
                dltContainer
                        .getAssignedPartitions()
                        .isEmpty()
                        &&
                        System.currentTimeMillis()
                                < assignmentDeadline
        ) {

            Thread.sleep(100);
        }

        assertFalse(
                dltContainer
                        .getAssignedPartitions()
                        .isEmpty(),
                "DLT consumer did not receive partition assignment"
        );


        try {

            String poisonEventId =
                    "poison-"
                            + System.nanoTime();

            String goodEventId =
                    "good-"
                            + System.nanoTime();


            /*
             * Both records use the SAME Kafka key.
             *
             * Therefore both must go to the same partition.
             */
            String key =
                    "product-777";


            String poisonPayload =
                    """
                    {
                      "eventId": "%s",
                      "eventType": "PRODUCT_PURCHASED",
                      "productId": 777,
                      "quantity": 1,
                      "occurredAt": "2026-09-05T20:00:00Z"
                    }
                    """.formatted(poisonEventId);


            String goodPayload =
                    """
                    {
                      "eventId": "%s",
                      "eventType": "PRODUCT_PURCHASED",
                      "productId": 777,
                      "quantity": 1,
                      "occurredAt": "2026-09-05T20:00:01Z"
                    }
                    """.formatted(goodEventId);


            AtomicInteger poisonAttempts =
                    new AtomicInteger(0);

            AtomicInteger goodAttempts =
                    new AtomicInteger(0);


            /*
             * Configure business processing behavior.
             *
             * Poison event:
             * always fails.
             *
             * Good event:
             * succeeds immediately.
             */
            doAnswer(invocation -> {

                OrderEvent event =
                        invocation.getArgument(0);

                if (event == null) {
                    return null;
                }


                if (poisonEventId.equals(
                        event.getEventId()
                )) {

                    int attempt =
                            poisonAttempts.incrementAndGet();

                    System.out.println(
                            "Poison event attempt #"
                                    + attempt
                    );

                    throw new RuntimeException(
                            "Permanent simulated failure"
                    );
                }


                if (goodEventId.equals(
                        event.getEventId()
                )) {

                    int attempt =
                            goodAttempts.incrementAndGet();

                    System.out.println(
                            "Good event processed - attempt #"
                                    + attempt
                    );

                    return null;
                }


                /*
                 * Ignore records unrelated to this test.
                 */
                return null;

            })
                    .when(processingService)
                    .process(
                            any(OrderEvent.class)
                    );


            /*
             * IMPORTANT:
             *
             * Do not publish until the real
             * OrderEventConsumer has joined Kafka
             * and owns a partition.
             */
            waitForMainKafkaListenerAssignment();


            /*
             * =====================================================
             * SEND POISON EVENT
             * =====================================================
             */
            var poisonSendResult =
                    kafkaTemplate
                            .send(
                                    "order-events",
                                    key,
                                    poisonPayload
                            )
                            .get(
                                    10,
                                    TimeUnit.SECONDS
                            );


            int poisonPartition =
                    poisonSendResult
                            .getRecordMetadata()
                            .partition();

            long poisonOffset =
                    poisonSendResult
                            .getRecordMetadata()
                            .offset();


            System.out.println(
                    "\n========== POISON EVENT SENT =========="
            );

            System.out.println(
                    "Event ID  : "
                            + poisonEventId
            );

            System.out.println(
                    "Partition : "
                            + poisonPartition
            );

            System.out.println(
                    "Offset    : "
                            + poisonOffset
            );

            System.out.println(
                    "=======================================\n"
            );


            /*
             * The poison event should:
             *
             * attempt #1
             * retry #1
             * retry #2
             * DLT
             */
            ConsumerRecord<String, String> poisonDltRecord =
                    waitForSpecificDltRecord(
                            dltRecords,
                            poisonEventId,
                            15,
                            TimeUnit.SECONDS
                    );


            assertNotNull(
                    poisonDltRecord,
                    "Poison event should eventually reach the DLT"
            );


            assertEquals(
                    key,
                    poisonDltRecord.key(),
                    "Poison event Kafka key should be preserved in DLT"
            );


            assertEquals(
                    3,
                    poisonAttempts.get(),
                    "Expected poison event to be processed 3 times"
            );


            /*
             * =====================================================
             * SEND GOOD EVENT
             * =====================================================
             *
             * This occurs after the poison event has been
             * successfully recovered to the DLT.
             */
            var goodSendResult =
                    kafkaTemplate
                            .send(
                                    "order-events",
                                    key,
                                    goodPayload
                            )
                            .get(
                                    10,
                                    TimeUnit.SECONDS
                            );


            int goodPartition =
                    goodSendResult
                            .getRecordMetadata()
                            .partition();

            long goodOffset =
                    goodSendResult
                            .getRecordMetadata()
                            .offset();


            System.out.println(
                    "\n========== GOOD EVENT SENT =========="
            );

            System.out.println(
                    "Event ID  : "
                            + goodEventId
            );

            System.out.println(
                    "Partition : "
                            + goodPartition
            );

            System.out.println(
                    "Offset    : "
                            + goodOffset
            );

            System.out.println(
                    "=====================================\n"
            );


            /*
             * Same key must produce to the same partition.
             */
            assertEquals(
                    poisonPartition,
                    goodPartition,
                    "Both events must be on the same partition"
            );


            /*
             * The second event must appear later in
             * that same partition.
             */
            assertTrue(
                    goodOffset > poisonOffset,
                    "Good event should have a later offset"
            );


            /*
             * Wait for Event B to actually be consumed.
             */
            long goodProcessingDeadline =
                    System.currentTimeMillis()
                            + 10_000;


            while (
                    goodAttempts.get() < 1
                            &&
                            System.currentTimeMillis()
                                    < goodProcessingDeadline
            ) {

                Thread.sleep(100);
            }


            /*
             * This proves the poison message did NOT
             * permanently block the Kafka partition.
             */
            assertEquals(
                    1,
                    goodAttempts.get(),
                    "Good event should be processed after poison event recovery"
            );


            System.out.println(
                    "\n========== POISON MESSAGE RESULT =========="
            );

            System.out.println(
                    "Poison Event Partition : "
                            + poisonPartition
            );

            System.out.println(
                    "Poison Event Offset    : "
                            + poisonOffset
            );

            System.out.println(
                    "Poison Attempts        : "
                            + poisonAttempts.get()
            );

            System.out.println(
                    "Poison Event           : DLT"
            );

            System.out.println();

            System.out.println(
                    "Good Event Partition   : "
                            + goodPartition
            );

            System.out.println(
                    "Good Event Offset      : "
                            + goodOffset
            );

            System.out.println(
                    "Good Event Attempts    : "
                            + goodAttempts.get()
            );

            System.out.println(
                    "Good Event             : SUCCESS"
            );

            System.out.println();

            System.out.println(
                    "Partition continued after poison record: YES"
            );

            System.out.println(
                    "===========================================\n"
            );

        } finally {

            dltContainer.stop();
        }
    }


    /*
     * Search the shared DLT queue for THIS
     * logical event only.
     *
     * Records created by other tests are ignored.
     */
    private ConsumerRecord<String, String>
    waitForSpecificDltRecord(
            BlockingQueue<ConsumerRecord<String, String>> records,
            String eventId,
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


            ConsumerRecord<String, String> record =
                    records.poll(
                            Math.min(
                                    remaining,
                                    500
                            ),
                            TimeUnit.MILLISECONDS
                    );


            if (record == null) {
                continue;
            }


            /*
             * Do not accept unrelated DLT messages.
             */
            if (
                    record.value() != null
                            &&
                            record
                                    .value()
                                    .contains(eventId)
            ) {

                return record;
            }
        }


        return null;
    }
}