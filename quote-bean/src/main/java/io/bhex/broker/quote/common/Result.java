package io.bhex.broker.quote.common;

import io.bhex.broker.quote.enums.ErrorCodeEnum;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
@EqualsAndHashCode
@Builder
public class Result<T> {

    private int code;
    private T data;
    private String msg;

    public static <T> Result<T> success(T t) {
        return Result.<T>builder()
            .code(Integer.parseInt(ErrorCodeEnum.SUCCESS.getCode()))
            .data(t)
            .build();
    }

    public static <T> Result<T> webSuccess(T t) {
        return Result.<T>builder()
            .code(Integer.parseInt(ErrorCodeEnum.WEB_SUCCESS.getCode()))
            .data(t)
            .build();
    }

    public static Result<SystemError> fail() {
        return Result.<SystemError>builder()
            .code(Integer.parseInt(ErrorCodeEnum.SYSTEM_ERROR.getCode()))
            .msg(ErrorCodeEnum.SYSTEM_ERROR.getDesc())
            .build();
    }

    public static <T> Result<T> fail(ErrorCodeEnum en) {
        return Result.<T>builder()
            .code(Integer.parseInt(en.getCode()))
            .msg(en.getDesc())
            .build();
    }

    public static <T> Result<T> fail(int code, String message) {
        return Result.<T>builder()
            .code(code)
            .msg(message)
            .build();
    }

    public static <T> Result<T> fail(String code, String message) {
        return fail(Integer.parseInt(code), message);
    }

}
