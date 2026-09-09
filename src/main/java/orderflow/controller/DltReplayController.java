package orderflow.controller;

import lombok.RequiredArgsConstructor;

import orderflow.service.DltReplayService;
import orderflow.service.DltReplayService.ReplayResult;

import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;

import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;


@RestController
@RequestMapping("/admin/dlt")
@RequiredArgsConstructor
public class DltReplayController {

    private final DltReplayService dltReplayService;


    /*
     * Example:
     *
     * POST /admin/dlt/replay/1
     */
    @PostMapping("/replay/{failedEventId}")
    public ResponseEntity<ReplayResult> replay(
            @PathVariable
            Long failedEventId
    ) {

        ReplayResult result =
                dltReplayService.replay(
                        failedEventId
                );


        if (result.success()) {

            return ResponseEntity
                    .ok(result);
        }


        return ResponseEntity
                .status(
                        HttpStatus.CONFLICT
                )
                .body(result);
    }
}