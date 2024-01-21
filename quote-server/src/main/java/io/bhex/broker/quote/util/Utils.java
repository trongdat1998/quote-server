package io.bhex.broker.quote.util;

import io.netty.channel.ChannelHandlerContext;
import io.netty.util.Attribute;
import io.netty.util.AttributeKey;

import java.util.Objects;

import static io.bhex.broker.quote.enums.StringsConstants.QUOTE_WS_VERSION;

public class Utils {

    public static Integer getWsVersion(ChannelHandlerContext ctx) {
        Attribute<Integer> attr = ctx.channel().attr(AttributeKey.valueOf(QUOTE_WS_VERSION));
        Integer version = attr.get();
        if (Objects.isNull(version)) {
            return 1;
        }
        return version;
    }
}
