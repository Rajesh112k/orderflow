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
         * Wait for our DLT observer to be ready.
         */
        long assignmentDeadline =
                System.currentTimeMillis() + 10_000;

        while (
                dltContainer.getAssignedPartitions().isEmpty()
                        &&
                        System.currentTimeMillis() < assignmentDeadline
        ) {

            Thread.sleep(100);
        }

        assertFalse(
                dltContainer.getAssignedPartitions().isEmpty(),
                "DLT consumer did not receive partition assignment"
        );


        try {

            /*
             * Give the application's Kafka listener
             * a short startup window.
             */
            Thread.sleep(2000);


            /*
             * Event A will permanently fail.
             */
            String poisonEventId =
                    "poison-"
                            + System.nanoTime();


            /*
             * Event B will succeed.
             */
            String goodEventId =
                    "good-"
                            + System.nanoTime();


            /*
             * SAME KEY.
             *
             * Therefore both records should be sent
             * to the same Kafka partition.
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
             * Configure our mocked business service.
             */
            doAnswer(invocation -> {

                OrderEvent event =
                        invocation.getArgument(0);


                /*
                 * EVENT A
                 *
                 * Always fail.
                 */
                if (
                        poisonEventId.equals(
                                event.getEventId()
                        )
                ) {

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


                /*
                 * EVENT B
                 *
                 * Always succeed.
                 */
                if (
                        goodEventId.equals(
                                event.getEventId()
                        )
                ) {

                    int attempt =
                            goodAttempts.incrementAndGet();


                    System.out.println(
                            "Good event processed - attempt #"
                                    + attempt
                    );


                    return null;
                }


                /*
                 * Ignore unrelated events.
                 */
                return null;

            })
                    .when(processingService)
                    .process(
                            any(OrderEvent.class)
                    );


            /*
             * =====================================================
             * SEND POISON EVENT FIRST
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
                    "Event ID  : " + poisonEventId
            );

            System.out.println(
                    "Partition : " + poisonPartition
            );

            System.out.println(
                    "Offset    : " + poisonOffset
            );

            System.out.println(
                    "=======================================\n"
            );


            /*
             * Wait until the poison event appears
             * in the DLT.
             *
             * This means:
             *
             * attempt #1 failed
             * retry #1 failed
             * retry #2 failed
             * recovery happened
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


            /*
             * Based on our configured:
             *
             * FixedBackOff(2000L, 2L)
             *
             * we expect:
             *
             * 1 initial attempt
             * +
             * 2 retries
             * =
             * 3 total attempts.
             */
            assertEquals(
                    3,
                    poisonAttempts.get(),
                    "Expected poison event to be processed 3 times"
            );


            /*
             * =====================================================
             * SEND GOOD EVENT SECOND
             * =====================================================
             *
             * We intentionally send this only AFTER we know
             * Event A was recovered to the DLT.
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
                    "Event ID  : " + goodEventId
            );

            System.out.println(
                    "Partition : " + goodPartition
            );

            System.out.println(
                    "Offset    : " + goodOffset
            );

            System.out.println(
                    "=====================================\n"
            );


            /*
             * This assertion is extremely important.
             *
             * We are trying to prove that the consumer
             * moved forward within the SAME partition.
             */
            assertEquals(
                    poisonPartition,
                    goodPartition,
                    "Both events must be on the same partition"
            );


            /*
             * Since Event B was produced later to the
             * same partition, its offset must be greater.
             */
            assertTrue(
                    goodOffset > poisonOffset,
                    "Good event should have a later offset"
            );


            /*
             * Now wait for Event B to actually be processed.
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
             * THIS is the core assertion.
             *
             * If the poison record permanently blocked
             * the partition, this would remain zero.
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
     * Search the DLT queue for one particular
     * logical event.
     *
     * Other DLT records are ignored.
     */
    private ConsumerRecord<String, String> waitForSpecificDltRecord(
            BlockingQueue<ConsumerRecord<String, String>> records,
            String eventId,
            long timeout,
            TimeUnit timeUnit
    )
            throws InterruptedException {

        long deadline =
                System.currentTimeMillis()
                        + timeUnit.toMillis(
                        timeout
                );


        while (
                System.currentTimeMillis()
                        < deadline
        ) {

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


            if (
                    record.value() != null
                            &&
                            record
                                    .value()
                                    .contains(
                                            eventId
                                    )
            ) {

                return record;
            }
        }


        return null;
    }
}