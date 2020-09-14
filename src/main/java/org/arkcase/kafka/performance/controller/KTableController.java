package main.java.org.arkcase.kafka.performance.controller;

import org.arkcase.kafka.performance.service.KTableReaderService;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RestController;

/**
 * Created by Vladimir Cherepnalkovski <vladimir.cherepnalkovski@armedia.com> on Feb, 2020
 */
@RestController
public class KTableController
{
    private final KTableReaderService kTableReaderService;

    public KTableController(KTableReaderService kTableReaderService) {
        this.kTableReaderService = kTableReaderService;
    }

    @GetMapping("/users/{user-id}")
    public ResponseEntity getUser(@PathVariable("user-id") String userId)
    {
        try
        {
            String result = kTableReaderService.getUser(userId);
            return ResponseEntity.ok(result);
        } catch (Exception e)
        {
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body("Unable to get user with id " + userId);
        }
    }

    @PostMapping("/users/delete/{user-id}")
    public ResponseEntity deleteUser(@PathVariable("user-id") String userId)
    {
        kTableReaderService.deleteUser(userId);
        return ResponseEntity.ok("Deleted user with id " + userId);
    }
}