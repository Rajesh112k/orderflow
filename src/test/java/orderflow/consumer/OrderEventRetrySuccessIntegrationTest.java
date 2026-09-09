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
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.Mockito.*;

@SpringBootTest(
        properties = {
                "spring.kafka.listener.auto-startup=true",
                "spring.kafka.consumer.auto-offset-reset=latest",
                "orderflow.kafka.consumer.group-id=orderflow-retry-success-main-consumer"
        }
)
@ActiveProfiles("test")
class OrderEventRetrySuccessIntegrationTest {

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
                "orderflow-retry-success-dlt-"
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
    void shouldSucceedOnRetryAndNotSendEventToDlt()
            throws Exception {

        BlockingQueue<ConsumerRecord<String, String>> dltRecords =
                new LinkedBlockingQueue<>();


        ConsumerFactory<String, String> dltConsumerFactory =
                createDltConsumerFactory();


        ContainerProperties containerProperties =
                new ContainerProperties(
                        "order-events.DLT"
                );


        KafkaMessageListenerContainer<String, String> dltContainer =
                new KafkaMessageListenerContainer<>(
                        dltConsumerFactory,
                        containerProperties
                );


        dltContainer.setupMessageListener(
                (MessageListener<String, String>) record ->
                        dltRecords.add(record)
        );


        dltContainer.start();


        /*
         * Wait for DLT consumer assignment.
         */
        long dltAssignmentDeadline =
                System.currentTimeMillis() + 10_000;


        while (
                dltContainer
                        .getAssignedPartitions()
                        .isEmpty()
                        &&
                        System.currentTimeMillis()
                                < dltAssignmentDeadline
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

            /*
             * Give the application's @KafkaListener
             * time to finish joining its test consumer group.
             */
            Thread.sleep(2000);


            String eventId =
                    "retry-success-test-"
                            + System.nanoTime();


            String key =
                    "product-456";


            String payload =
                    """
                    {
                      "eventId": "%s",
                      "eventType": "PRODUCT_PURCHASED",
                      "productId": 456,
                      "quantity": 1,
                      "occurredAt": "2026-09-05T19:00:00Z"
                    }
                    """.formatted(eventId);


            /*
             * Tracks only our event's processing attempts.
             */
            AtomicInteger attempts =
                    new AtomicInteger(0);


            /*
             * Instead of:
             *
             * doThrow(...)
             *     .doNothing()
             *
             * we explicitly control what happens
             * on every call.
             */
            doAnswer(invocation -> {

                OrderEvent event =
                        invocation.getArgument(0);


                /*
                 * Ignore unrelated Kafka records.
                 */
                if (
                        event == null
                                ||
                                !eventId.equals(
                                        event.getEventId()
                                )
                ) {

                    return null;
                }


                /*
                 * Increment attempt number.
                 */
                int attempt =
                        attempts.incrementAndGet();


                System.out.println(
                        "\nProcessing event "
                                + eventId
                                + " - attempt #"
                                + attempt
                );


                /*
                 * FIRST attempt fails.
                 */
                if (attempt == 1) {

                    System.out.println(
                            "Attempt #1 intentionally failing"
                    );


                    throw new RuntimeException(
                            "Simulated temporary processing failure"
                    );
                }


                /*
                 * SECOND attempt succeeds.
                 */
                System.out.println(
                        "Attempt #"
                                + attempt
                                + " succeeded"
                );


                return null;

            })
                    .when(processingService)
                    .process(
                            any(OrderEvent.class)
                    );


            System.out.println(
                    "\nSending retry-success event..."
            );

            System.out.println(
                    "Event ID : " + eventId
            );

            System.out.println(
                    "Kafka Key: " + key
            );


            var sendResult =
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
             * Wait for the retry.
             */
            long retryDeadline =
                    System.currentTimeMillis()
                            + 15_000;


            while (
                    attempts.get() < 2
                            &&
                            System.currentTimeMillis()
                                    < retryDeadline
            ) {

                Thread.sleep(100);
            }


            System.out.println(
                    "\nAttempts observed: "
                            + attempts.get()
            );


            /*
             * We expect exactly:
             *
             * attempt #1 -> failed
             * attempt #2 -> succeeded
             */
            assertEquals(
                    2,
                    attempts.get(),
                    "Expected first processing attempt to fail "
                            + "and second attempt to succeed"
            );


            verify(
                    processingService,
                    times(2)
            )
                    .process(
                            argThat(event ->
                                    event != null
                                            &&
                                            eventId.equals(
                                                    event.getEventId()
                                            )
                            )
                    );


            /*
             * After success, make sure OUR event
             * was not published to the DLT.
             */
            boolean reachedDlt =
                    waitForEventInDlt(
                            dltRecords,
                            key,
                            eventId,
                            4,
                            TimeUnit.SECONDS
                    );


            assertFalse(
                    reachedDlt,
                    "Event succeeded on retry and should not reach DLT"
            );


            System.out.println(
                    "\n========== RETRY SUCCESS RESULT =========="
            );

            System.out.println(
                    "Event ID              : "
                            + eventId
            );

            System.out.println(
                    "Processing attempt #1 : FAILED"
            );

            System.out.println(
                    "Processing attempt #2 : SUCCEEDED"
            );

            System.out.println(
                    "Total attempts        : "
                            + attempts.get()
            );

            System.out.println(
                    "DLT for this event     : NONE"
            );

            System.out.println(
                    "==========================================\n"
            );

        } finally {

            dltContainer.stop();
        }
    }


    private boolean waitForEventInDlt(
            BlockingQueue<ConsumerRecord<String, String>> dltRecords,
            String expectedKey,
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
                    dltRecords.poll(
                            Math.min(
                                    remaining,
                                    250
                            ),
                            TimeUnit.MILLISECONDS
                    );


            if (record == null) {
                continue;
            }


            /*
             * Ignore DLT events belonging
             * to other Kafka records.
             */
            if (
                    !expectedKey.equals(
                            record.key()
                    )
            ) {

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

                return true;
            }
        }


        return false;
    }
}