package orderflow.repository;

import orderflow.model.ProcessedEvent;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;

import java.time.Instant;

public interface ProcessedEventRepository
        extends JpaRepository<ProcessedEvent, String> {

    @Modifying
    @Query(
            value = """
                INSERT INTO processed_events
                    (event_id, processed_at)
                VALUES
                    (:eventId, :processedAt)
                ON CONFLICT (event_id) DO NOTHING
                """,
            nativeQuery = true
    )
    int insertIfNotExists(
            @Param("eventId") String eventId,
            @Param("processedAt") Instant processedAt
    );
}