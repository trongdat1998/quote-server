package io.bhex.broker.quote.controller.api;

import io.bhex.broker.quote.data.dto.api.ServerTimeDTO;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("${bhex.api.prefix}")
public class ApiServerController {

    @GetMapping("/time")
    public ServerTimeDTO serverTime() {
        return ServerTimeDTO.builder()
            .serverTime(System.currentTimeMillis())
            .build();
    }

    private static final Object PONG = new Object();

    @GetMapping("/ping")
    public Object ping() {
        return PONG;
    }
}
