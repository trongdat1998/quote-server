package io.bhex.broker.quote.enums;

import java.util.Arrays;

public enum EventEnum {

    SUB("sub"),
    CANCEL("cancel"),
    CANCEL_ALL("cancel_all"),
    ;

    String event;
    EventEnum(String event) {
        this.event = event;
    }

    public static EventEnum valueOF(String type) {
        return Arrays.stream(EventEnum.values())
            .filter(eventEnum -> eventEnum.event.equalsIgnoreCase(type))
            .findFirst()
            .orElse(null);
    }
}
