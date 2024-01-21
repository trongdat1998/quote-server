package io.bhex.broker.quote.controller.web;

import io.bhex.broker.quote.common.Result;
import io.bhex.broker.quote.data.dto.web.ServerTimeDTO;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("${bhex.inner-api.prefix}")
public class ServerController {

    @GetMapping("/time")
    public Result<ServerTimeDTO> serverTime() {
        return Result.success(ServerTimeDTO.builder()
            .serverTime(System.currentTimeMillis())
            .build());
    }
}
