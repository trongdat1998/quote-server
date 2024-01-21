package io.bhex.broker.quote.exception;

import lombok.Data;
import lombok.EqualsAndHashCode;
import org.apache.commons.lang3.StringUtils;

@Data
@EqualsAndHashCode(callSuper=false)
public class HttpException extends Exception {
    private int status;
    private String msg;

    public HttpException(int status) {
        super();
        this.status = status;
        this.msg = StringUtils.EMPTY;
    }

    public HttpException(int status, String msg) {
        super();
        this.status = status;
        this.msg = msg;
    }
}
