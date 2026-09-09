package orderflow.service;

import orderflow.model.FailedEvent;
import orderflow.model.FailedEventStatus;
import orderflow.repository.FailedEventRepository;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.test.context.ActiveProfiles;

import java.time.Instant;

import static org.junit.jupiter.api.Assertions.*;

@SpringBootTest
@ActiveProfiles("test")
class DltReplayServiceIntegrationTest {

    @Autowired
    private DltReplayService dltReplayService;

    @Autowired
    private FailedEventRepository failedEventRepository;

    private Long failedEventId;

    @BeforeEach
    void setup() {

        failedEventRepository.deleteAll();

        FailedEvent event = FailedEvent.builder()
                .eventId("replay-test-event")
                .originalTopic("order-events")
                .originalPartition(0)
                .originalOffset(999L)
                .originalKey("1")
                .payload("""
                        {
                          "eventId":"replay-test-event",
                          "eventType":"PRODUCT_PURCHASED",
                          "productId":1,
                          "quantity":1,
                          "occurredAt":"2026-09-06T03:00:00Z"
                        }
                        """)
                .status(FailedEventStatus.FAILED)
                .replayCount(0)
                .createdAt(Instant.now())
                .build();

        failedEventId =
                failedEventRepository.save(event).getId();
    }

    @Test
    void shouldReplayFailedEvent() {

        DltReplayService.ReplayResult result =
                dltReplayService.replay(failedEventId);

        assertTrue(result.success());

        FailedEvent updated =
                failedEventRepository
                        .findById(failedEventId)
                        .orElseThrow();

        assertEquals(
                FailedEventStatus.REPLAYED,
                updated.getStatus()
        );

        assertEquals(
                1,
                updated.getReplayCount()
        );

        assertNotNull(
                updated.getLastReplayedAt()
        );

        assertNull(
                updated.getReplayStartedAt()
        );
    }
}