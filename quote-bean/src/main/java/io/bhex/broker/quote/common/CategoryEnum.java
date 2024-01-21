package io.bhex.broker.quote.common;

public enum CategoryEnum {
    SPOT(1),
    OPTION(3),
    FUTURE(4);

    private int category;

    CategoryEnum(int category) {
        this.category = category;
    }

    public int getCategory() {
        return category;
    }
}
