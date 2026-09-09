package orderflow.producer;

import orderflow.event.OrderEvent;

import lombok.RequiredArgsConstructor;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

@Service
@RequiredArgsConstructor
public class OrderEventProducer {

    private static final String TOPIC = "order-events";

    private final KafkaTemplate<String, OrderEvent> kafkaTemplate;

    public void publish(OrderEvent event) {

        String key = event.getProductId().toString();

        var future = kafkaTemplate.send(
                TOPIC,
                key,
                event
        );

        future.whenComplete((result, exception) -> {

            if (exception != null) {
                System.out.println(
                        "Kafka publish failed: "
                                + exception.getMessage()
                );

                return;
            }

            System.out.println(
                    "Kafka publish succeeded. "
                            + "Topic: "
                            + result.getRecordMetadata().topic()
                            + ", Partition: "
                            + result.getRecordMetadata().partition()
                            + ", Offset: "
                            + result.getRecordMetadata().offset()
            );
        });
    }
}