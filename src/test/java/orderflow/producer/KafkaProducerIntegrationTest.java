package orderflow.producer;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;

import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.jupiter.api.Test;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.KafkaMessageListenerContainer;
import org.springframework.kafka.listener.ContainerProperties;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.kafka.listener.MessageListener;

import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;

@SpringBootTest
@ActiveProfiles("test")
class KafkaProducerIntegrationTest {

    @Autowired
    private KafkaTemplate<String, String> kafkaTemplate;

    @Autowired
    @Qualifier("testConsumerFactory")
    private ConsumerFactory<String, String> consumerFactory;


    @TestConfiguration
    static class KafkaTestConfig {

        @Bean
        ConsumerFactory<String, String> testConsumerFactory() {

            Map<String, Object> props = new HashMap<>();

            props.put(
                    ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,
                    "localhost:9092"
            );

            props.put(
                    ConsumerConfig.GROUP_ID_CONFIG,
                    "orderflow-integration-test-" + UUID.randomUUID()
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
    }
    @Test
    void shouldSendAndReceiveMessageThroughRealKafka()
            throws Exception {

        BlockingQueue<ConsumerRecord<String, String>> records =
                new LinkedBlockingQueue<>();

        ContainerProperties containerProperties =
                new ContainerProperties("order-events");

        KafkaMessageListenerContainer<String, String> container =
                new KafkaMessageListenerContainer<>(
                        consumerFactory,
                        containerProperties
                );

        container.setupMessageListener(
                (MessageListener<String, String>) record ->
                        records.add(record)
        );

        container.start();

        // 2. Wait until Kafka assigns partition(s) to this consumer
        long deadline =
                System.currentTimeMillis() + 10_000;

        while (
                container.getAssignedPartitions().isEmpty()
                        &&
                        System.currentTimeMillis() < deadline
        ) {
            Thread.sleep(100);
        }

        // 3. Make sure the consumer actually became ready
        assertFalse(
                container.getAssignedPartitions().isEmpty(),
                "Kafka consumer did not receive a partition assignment"
        );

        try {

            // 4. NOW send the message
            String key = "product-123";

            String message =
                    """
                    {
                      "eventId": "integration-test-123",
                      "eventType": "PRODUCT_PURCHASED",
                      "productId": 123
                    }
                    """;

            kafkaTemplate
                    .send(
                            "order-events",
                            key,
                            message
                    )
                    .get(10, TimeUnit.SECONDS);

            // 5. Wait for the consumer to receive it
            ConsumerRecord<String, String> receivedRecord =
                    records.poll(
                            10,
                            TimeUnit.SECONDS
                    );

            // 6. Verify
            assertNotNull(receivedRecord);

            assertEquals(
                    key,
                    receivedRecord.key()
            );

            assertEquals(
                    message,
                    receivedRecord.value()
            );

        } finally {

            // 7. Always stop our test consumer
            container.stop();
        }
    }

    @Test
    void shouldSendRecordsWithSameKeyToSamePartition()
            throws Exception {

        String key = "product-123";

        var firstResult =
                kafkaTemplate
                        .send(
                                "order-events",
                                key,
                                "event-1"
                        )
                        .get(10, TimeUnit.SECONDS);

        var secondResult =
                kafkaTemplate
                        .send(
                                "order-events",
                                key,
                                "event-2"
                        )
                        .get(10, TimeUnit.SECONDS);

        var thirdResult =
                kafkaTemplate
                        .send(
                                "order-events",
                                key,
                                "event-3"
                        )
                        .get(10, TimeUnit.SECONDS);


        int firstPartition =
                firstResult.getRecordMetadata().partition();

        int secondPartition =
                secondResult.getRecordMetadata().partition();

        int thirdPartition =
                thirdResult.getRecordMetadata().partition();


        assertEquals(
                firstPartition,
                secondPartition
        );

        assertEquals(
                secondPartition,
                thirdPartition
        );
    }

    @Test
    void shouldExposePartitionAndOffsetMetadata()
            throws Exception {

        String key = "product-999";

        var result =
                kafkaTemplate
                        .send(
                                "order-events",
                                key,
                                "metadata-test-event"
                        )
                        .get(10, TimeUnit.SECONDS);

        int partition =
                result.getRecordMetadata().partition();

        long offset =
                result.getRecordMetadata().offset();

        String topic =
                result.getRecordMetadata().topic();

        System.out.println(
                "Topic: " + topic
                        + ", Partition: " + partition
                        + ", Offset: " + offset
        );

        assertEquals(
                "order-events",
                topic
        );

        assertTrue(
                partition >= 0
        );

        assertTrue(
                offset >= 0
        );
    }
}