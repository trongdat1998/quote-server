package io.bhex.broker.quote.enums;

import java.util.Arrays;

public enum EventV2Enum {

    SUB("sub"),
    CANCEL("cancel"),
    ;

    String event;
    EventV2Enum(String event) {
        this.event = event;
    }

    public static EventV2Enum valueOF(String type) {
        return Arrays.stream(EventV2Enum.values())
            .filter(eventEnum -> eventEnum.event.equalsIgnoreCase(type))
            .findFirst()
            .orElse(null);
    }
}
