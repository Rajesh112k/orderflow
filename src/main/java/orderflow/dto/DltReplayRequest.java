package orderflow.dto;

import jakarta.validation.constraints.NotBlank;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;


@Getter
@Setter
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class DltReplayRequest {

    /*
     * Original Kafka key.
     *
     * Example:
     *
     * "777"
     *
     * We preserve this during replay so the normal
     * Kafka partitioning strategy can use the same key.
     */
    @NotBlank(message = "Kafka key is required")
    private String key;


    /*
     * Original JSON payload from the DLT.
     */
    @NotBlank(message = "Payload is required")
    private String payload;
}