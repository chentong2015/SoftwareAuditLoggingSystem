package com.audit.server;

import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RestController;

@RestController
public class AuditController {

    @GetMapping("/v1/log/{markId}")
    public String logAudit(@PathVariable("markId") int markId) {
        if (markId > 5) {
            return "success";
        }
        return "failed";
    }
}
