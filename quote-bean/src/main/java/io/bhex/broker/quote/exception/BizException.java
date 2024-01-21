package io.bhex.broker.quote.exception;

import io.bhex.broker.quote.enums.ErrorCodeEnum;
import lombok.Getter;

public class BizException extends Exception {
    @Getter
    private ErrorCodeEnum errorCodeEnum;

    public BizException(ErrorCodeEnum errorCodeEnum) {
        super(errorCodeEnum.name());
        this.errorCodeEnum = errorCodeEnum;
    }
}
