package io.bhex.broker.quote.controller.api;

import lombok.extern.slf4j.Slf4j;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.core.io.ResourceLoader;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import javax.annotation.PostConstruct;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;

@RequestMapping("/")
@Slf4j
@RestController
public class ApiIndexController {
    private final ResourceLoader resourceLoader;
    private String res = StringUtils.EMPTY;

    public ApiIndexController(@Qualifier("createResourceLoader") ResourceLoader resourceLoader) {
        this.resourceLoader = resourceLoader;
    }

    @PostConstruct
    public void setUp() {

        InputStream inputStream = this.getClass().getClassLoader().getResourceAsStream("api.json");
        String content = null;
        try {
            content = IOUtils.toString(inputStream, StandardCharsets.UTF_8);
        } catch (IOException e) {
            log.error(e.getMessage(), e);
        }
        content = StringUtils.replaceAll(content, " ", StringUtils.EMPTY);
        content = StringUtils.replaceAll(content, "\n", StringUtils.EMPTY);
        res = content;
    }


    @RequestMapping("/")
    public Object apiList() throws IOException {
        return res;
    }
}
