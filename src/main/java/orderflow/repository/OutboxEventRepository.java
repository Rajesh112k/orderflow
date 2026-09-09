package orderflow.repository;

import orderflow.model.OutboxEvent;
import orderflow.model.OutboxStatus;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;

import java.time.Instant;
import java.util.List;

public interface OutboxEventRepository
        extends JpaRepository<OutboxEvent, String> {

    List<OutboxEvent> findByStatus(OutboxStatus status);

    @Query(
            value = """
                SELECT *
                FROM outbox_events
                WHERE status = :status
                ORDER BY created_at
                LIMIT 10
                FOR UPDATE SKIP LOCKED
                """,
            nativeQuery = true
    )
    List<OutboxEvent> findPendingForUpdate(
            @Param("status") String status
    );

    @Query("""
       SELECT e
       FROM OutboxEvent e
       WHERE e.status = :status
       AND e.processingStartedAt < :cutoff
       """)
    List<OutboxEvent> findStaleProcessingEvents(
            @Param("status") OutboxStatus status,
            @Param("cutoff") Instant cutoff
    );

    @Query(
            value = """
            SELECT *
            FROM outbox_events
            WHERE status = 'PENDING'
              AND next_attempt_at <= CURRENT_TIMESTAMP
            ORDER BY next_attempt_at, created_at
            LIMIT 10
            FOR UPDATE SKIP LOCKED
            """,
            nativeQuery = true
    )
    List<OutboxEvent> findReadyEventsForUpdate();


    long countByAggregateIdAndEventType(
            Long aggregateId,
            String eventType
    );
}