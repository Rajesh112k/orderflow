package orderflow.controller;

import lombok.RequiredArgsConstructor;
import orderflow.event.OrderEvent;
import orderflow.producer.OrderEventProducer;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.time.Instant;
import java.util.UUID;

@RestController
@RequestMapping("/kafka-test")
@RequiredArgsConstructor
public class KafkaTestController {

    private final OrderEventProducer orderEventProducer;

    @PostMapping
    public ResponseEntity<String> publishTestEvent() {

        OrderEvent event = OrderEvent.builder()
                .eventId(UUID.randomUUID().toString())
                .eventType("PRODUCT_PURCHASED")
                .productId(1L)
                .quantity(1)
                .occurredAt(Instant.now())
                .build();

        orderEventProducer.publish(event);

        return ResponseEntity.ok("Kafka publish requested");
    }
}