package orderflow.service;

import orderflow.model.OutboxEvent;

import orderflow.repository.OutboxEventRepository;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;

import java.util.List;
import java.util.concurrent.CompletableFuture;

import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.*;

class OutboxPublisherTest {

    @Mock
    private OutboxClaimService outboxClaimService;

    @Mock
    private OutboxStatusService outboxStatusService;
    @Mock
    private OutboxEventRepository outboxEventRepository;

    @Mock
    private KafkaTemplate<String, String> kafkaTemplate;

    private OutboxPublisher outboxPublisher;

    @BeforeEach
    void setUp() {

        MockitoAnnotations.openMocks(this);

        outboxPublisher =
                new OutboxPublisher(
                        outboxClaimService,
                        outboxEventRepository,
                        kafkaTemplate,
                        outboxStatusService
                );
    }

    @Test
    void shouldMarkEventPublishedWhenKafkaSendSucceeds() {

        OutboxEvent event =
                OutboxEvent.builder()
                        .id("event-123")
                        .aggregateId(1L)
                        .payload("{\"eventId\":\"event-123\"}")
                        .build();

        when(outboxClaimService.claimPendingEvents())
                .thenReturn(List.of(event));

        CompletableFuture<SendResult<String, String>> future =
                CompletableFuture.completedFuture(null);

        when(kafkaTemplate.send(
                anyString(),
                anyString(),
                anyString()
        ))
                .thenReturn(future);

        outboxPublisher.publishPendingEvents();

        verify(outboxStatusService)
                .markPublished("event-123");

        verify(outboxStatusService, never())
                .markFailedAttempt("event-123");
    }

    @Test
    void shouldMarkFailedAttemptWhenKafkaSendFails() {

        OutboxEvent event =
                OutboxEvent.builder()
                        .id("event-456")
                        .aggregateId(1L)
                        .payload("{\"eventId\":\"event-456\"}")
                        .build();

        when(outboxClaimService.claimPendingEvents())
                .thenReturn(List.of(event));


        CompletableFuture<SendResult<String, String>> future =
                new CompletableFuture<>();

        future.completeExceptionally(
                new RuntimeException("Kafka unavailable")
        );


        when(kafkaTemplate.send(
                anyString(),
                anyString(),
                anyString()
        ))
                .thenReturn(future);


        outboxPublisher.publishPendingEvents();


        verify(outboxStatusService)
                .markFailedAttempt("event-456");

        verify(outboxStatusService, never())
                .markPublished("event-456");
    }
}