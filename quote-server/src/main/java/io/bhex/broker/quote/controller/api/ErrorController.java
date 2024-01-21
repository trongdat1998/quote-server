package io.bhex.broker.quote.controller.api;

import io.bhex.broker.quote.common.Result;
import io.bhex.broker.quote.exception.HttpException;
import org.apache.commons.lang3.StringUtils;
import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import javax.servlet.http.HttpServletRequest;

@RestController
public class ErrorController implements org.springframework.boot.web.servlet.error.ErrorController {

    private static final String ERROR_PATH = "/error";

    @RequestMapping(value = ERROR_PATH, produces = MediaType.APPLICATION_JSON_VALUE)
    public Result error(HttpServletRequest request) throws HttpException {
        Object status = request.getAttribute("javax.servlet.error.status_code");
        if (status == null) {
            status = 200;
        }
        Object reason = request.getAttribute("javax.servlet.error.message");
        if (reason == null) {
            reason = StringUtils.EMPTY;
        }
        return Result.fail((int) status, (String) reason);
    }


    @Override
    public String getErrorPath() {
        return ERROR_PATH;
    }
}
