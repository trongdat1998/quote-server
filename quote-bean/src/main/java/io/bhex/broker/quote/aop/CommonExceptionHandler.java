package io.bhex.broker.quote.aop;

import io.bhex.broker.quote.common.Result;
import io.bhex.broker.quote.enums.ErrorCodeEnum;
import io.bhex.broker.quote.exception.BizException;
import io.bhex.broker.quote.exception.HttpException;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.HttpStatus;
import org.springframework.web.HttpMediaTypeNotAcceptableException;
import org.springframework.web.bind.MissingServletRequestParameterException;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.ResponseStatus;
import org.springframework.web.bind.annotation.RestControllerAdvice;
import org.springframework.web.method.annotation.MethodArgumentTypeMismatchException;
import reactor.netty.channel.AbortedException;

import static org.springframework.http.HttpStatus.NOT_ACCEPTABLE;

@RestControllerAdvice(basePackages = "io.bhex.broker.quote.controller")
@Slf4j
public class CommonExceptionHandler {

    /******************
     * 4xx
     */
    @ExceptionHandler(MethodArgumentTypeMismatchException.class)
    @ResponseStatus(HttpStatus.BAD_REQUEST)
    public Result<Object> bindException(MethodArgumentTypeMismatchException e) {
        return Result.fail(ErrorCodeEnum.PARAM_TYPE_ERROR.getCode(),
            String.format(ErrorCodeEnum.PARAM_TYPE_ERROR.getDesc(), e.getName(), e.getParameter()
                .getParameterType()
                .getSimpleName()));
    }

    @ExceptionHandler(MissingServletRequestParameterException.class)
    @ResponseStatus(HttpStatus.BAD_REQUEST)
    public Result<Object> paramsMissing(MissingServletRequestParameterException e) {
        return Result.fail(ErrorCodeEnum.PARAM_REQUIRED.getCode(),
            String.format(ErrorCodeEnum.PARAM_REQUIRED.getDesc(),
                e.getParameterName(),
                e.getParameterType()));
    }

    @ExceptionHandler(BizException.class)
    @ResponseStatus(HttpStatus.BAD_REQUEST)
    public Result<String> handleQuoteError(BizException e) {
        return Result.fail(e.getErrorCodeEnum().getCode(), e.getErrorCodeEnum().getDesc());
    }

    @ExceptionHandler(HttpException.class)
    public Result<String> handleHttpError(HttpException e) {
        return Result.fail(e.getStatus(), e.getMsg());
    }


    @ExceptionHandler(HttpMediaTypeNotAcceptableException.class)
    @ResponseStatus(NOT_ACCEPTABLE)
    public Result<String> handleNotAcceptable(HttpMediaTypeNotAcceptableException e) {
        return Result.fail(NOT_ACCEPTABLE.value(), NOT_ACCEPTABLE.getReasonPhrase());
    }

    /********
     * 5xx
     */
    @ExceptionHandler(Throwable.class)
    @ResponseStatus(HttpStatus.INTERNAL_SERVER_ERROR)
    public Result handle500(Throwable e) {
        if (AbortedException.isConnectionReset(e)) {
            return Result.fail(ErrorCodeEnum.CLIENT_CONNECTION_RESET.getCode(),
                ErrorCodeEnum.CLIENT_CONNECTION_RESET.getDesc());
        }
        log.error("System error: ", e);
        return Result.fail();
    }
}
