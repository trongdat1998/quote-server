package io.bhex.broker.quote.util;

import com.fasterxml.jackson.annotation.JsonInclude.Include;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonParser.Feature;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JavaType;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.module.SimpleModule;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;

import java.io.IOException;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.List;

import static com.fasterxml.jackson.databind.DeserializationFeature.ACCEPT_EMPTY_STRING_AS_NULL_OBJECT;
import static com.fasterxml.jackson.databind.DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES;

/**
 * @author boyce
 */
@Slf4j
public class JsonUtils {

    /**
     * 不输出value为空的结点
     */
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper()
        .setSerializationInclusion(Include.NON_NULL)
        .enable(ACCEPT_EMPTY_STRING_AS_NULL_OBJECT)
        .configure(JsonParser.Feature.ALLOW_UNQUOTED_FIELD_NAMES, true)
        .configure(FAIL_ON_UNKNOWN_PROPERTIES, false)
        .configure(SerializationFeature.FAIL_ON_UNWRAPPED_TYPE_IDENTIFIERS, false)
        .configure(Feature.ALLOW_SINGLE_QUOTES, true)
        // BigDecimal要去尾零和写成string
        .registerModule(new SimpleModule().addSerializer(BigDecimal.class, new JsonSerializer<BigDecimal>() {
            @Override
            public void serialize(BigDecimal value, JsonGenerator gen, SerializerProvider serializers)
                throws IOException {
                gen.writeString(value.stripTrailingZeros()
                    .toPlainString());
            }
        }));

    public static String toJSONString(Object o) {
        try {
            return OBJECT_MAPPER.writeValueAsString(o);
        } catch (Throwable e) {
            //log.error("convert json error ", e.getMessage());
            log.error("convert json error ", e);
        }
        return StringUtils.EMPTY;
    }

    public static <T> T parseString(String jsonString, Class<T> tClass) {
        try {
            return OBJECT_MAPPER.readValue(jsonString, tClass);
        } catch (Throwable e) {
            log.error("parse json error ", e);
        }
        return null;
    }

    public static <T> List<T> parseStringToList(String listJson, Class<T> tClass) {
        JavaType javaType = OBJECT_MAPPER.getTypeFactory()
            .constructParametricType(ArrayList.class, tClass);
        try {
            return OBJECT_MAPPER.readValue(listJson, javaType);
        } catch (Throwable e) {
            log.error(" parse list error ", e);
        }
        return new ArrayList<>();
    }

    public static <T> List<T> parseStringToListObject(String json, Class<T> tClass, Class childClass) {
        JavaType javaType = OBJECT_MAPPER.getTypeFactory()
            .constructParametricType(tClass, childClass);
        JavaType parentType = OBJECT_MAPPER.getTypeFactory()
            .constructParametricType(List.class, javaType);
        try {
            return OBJECT_MAPPER.readValue(json, parentType);
        } catch (Throwable e) {
            log.error(" parse list object error ", e);
        }
        return new ArrayList<>();
    }

    public static <T> T parseJson(String json, TypeReference<T> typeReference) throws IOException {
        return OBJECT_MAPPER.readValue(json, typeReference);
    }

    public static <T> T convertObject(Object o, Class<T> clazz) {
        return OBJECT_MAPPER.convertValue(o, clazz);
    }

}
