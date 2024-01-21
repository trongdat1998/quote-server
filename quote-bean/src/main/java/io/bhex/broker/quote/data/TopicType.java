/*
* ***********************************
 * @项目名称: BlueHelix Exchange Project
 * @文件名称: EngineStatus
 * @Author essen 
 * @Copyright（C）: 2018 BlueHelix Inc.   All rights reserved.
 * 注意：本内容仅限于内部传阅，禁止外泄以及用于其他的商业目的。
 **************************************
 */
package io.bhex.broker.quote.data;

/**
 * Topic Type
 *
 * @author essen
 */
public enum TopicType {

    REAL_TIME(0, "realtimes", "实时行情"),
    KLINE(1, "kline", "KLine"),
    DEPTH(2, "depth", "深度"),
    TRADE(3, "trade", "最新成交");

    private Integer code;
    private String name;
    private String desc;

    TopicType(Integer code, String name, String desc) {
        this.code = code;
        this.name = name;
        this.desc = desc;
    }

    public Integer getCode() {
        return code;
    }

    public String getName() {
        return name;
    }

    public String getDesc() {
        return desc;
    }

    /**
     * Get a TopicType by name
     *
     * @param name the name of TopicType
     * @return return a TopicType, if does not exists, that will return a null value
     */
    public static TopicType get(String name) {

        try {
            TopicType[] ks = TopicType.values();
            for (TopicType k : ks) {
                if (k.getName().equals(name)) {
                    return k;
                }
            }
            return null;

        } catch (Exception ex) {
            return null;
        }
    }
}
