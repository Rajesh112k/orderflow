package orderflow.service;

import lombok.RequiredArgsConstructor;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

@Service
@RequiredArgsConstructor
public class FailedEventRecoveryScheduler {

    private final FailedEventReplayStateService replayStateService;

    @Scheduled(fixedDelay = 60000)
    public void recoverStaleReplays() {

        int recovered =
                replayStateService.recoverStaleReplays();

        if (recovered > 0) {
            System.out.println(
                    "Recovered "
                            + recovered
                            + " stale failed-event replay(s)"
            );
        }
    }
}