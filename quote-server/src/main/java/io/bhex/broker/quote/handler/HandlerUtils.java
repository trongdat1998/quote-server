package io.bhex.broker.quote.handler;

import io.bhex.broker.quote.data.ClientMessageV2;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

import static io.bhex.broker.quote.enums.StringsConstants.BINARY;

class HandlerUtils {

    static Map<String, String> getFilterCommonParams(ClientMessageV2 message) {
        if (Objects.isNull(message)) {
            return new HashMap<>();
        }

        Map<String, String> params = message.getParams();
        if (Objects.isNull(params)) {
            return new HashMap<>();
        }

        Map<String, String> filteredParams = new HashMap<>();
        if (params.containsKey(BINARY)) {
            filteredParams.put(BINARY, params.get(BINARY));
        }

        return filteredParams;
    }
}
