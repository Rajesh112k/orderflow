package orderflow.service;

import lombok.RequiredArgsConstructor;
import orderflow.event.OrderEvent;
import orderflow.repository.ProcessedEventRepository;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.Instant;

@Service
@RequiredArgsConstructor
public class OrderEventProcessingService {

    private final ProcessedEventRepository processedEventRepository;

    @Transactional
    public void process(OrderEvent event) {

        System.out.println(
                "Processing event: "
                        + event.getEventId()
        );

        /*
         * TEMPORARY TEST CONDITION
         *
         * Any event with productId = 999 will fail.
         * We are using this only to test:
         *
         * consumer failure
         *      ↓
         * retry
         *      ↓
         * retry
         *      ↓
         * DLT
         */
        if (Long.valueOf(999L).equals(event.getProductId())) {

            System.out.println(
                    "Simulating failure for event: "
                            + event.getEventId()
            );

            throw new RuntimeException(
                    "Simulated permanent failure"
            );
        }

        /*
         * IDEMPOTENCY CHECK
         *
         * Try to insert the eventId into processed_events.
         *
         * If the eventId already exists,
         * insertIfNotExists() returns 0.
         */
        int insertedRows =
                processedEventRepository.insertIfNotExists(
                        event.getEventId(),
                        Instant.now()
                );

        /*
         * If nothing was inserted,
         * this event was already processed earlier.
         */
        if (insertedRows == 0) {

            System.out.println(
                    "Duplicate event ignored: "
                            + event.getEventId()
            );

            return;
        }

        /*
         * Actual business processing would happen here.
         *
         * Examples:
         *
         * update inventory
         * create order record
         * send notification
         * update payment state
         */
        System.out.println(
                "Successfully processed event: "
                        + event.getEventId()
        );
    }
}