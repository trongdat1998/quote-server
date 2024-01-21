package io.bhex.broker.quote;

import io.bhex.broker.quote.client.QuoteInitializeManager;
import io.bhex.broker.quote.config.AppConfig;
import io.bhex.broker.quote.config.ConfigCenter;
import io.bhex.broker.quote.config.WebSocketConfig;
import io.bhex.broker.quote.job.BrokerInfoTask;
import io.bhex.broker.quote.job.MonitorTask;
import io.bhex.broker.quote.server.WebSocketInitializer;
import io.bhex.broker.quote.server.WebSocketServer;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.FilterType;
import org.springframework.scheduling.annotation.EnableScheduling;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;

@SpringBootApplication
@EnableScheduling
@ComponentScan(basePackages = "io.bhex", excludeFilters = @ComponentScan.Filter(type = FilterType.REGEX, pattern = "io.bhex.ex.quote.core.*"))
@Slf4j
public class QuoteWsApplication {

    private final BrokerInfoTask brokerInfoTask;
    private final ConfigCenter configCenter;
    private final WebSocketConfig webSocketConfig;
    private final WebSocketInitializer webSocketInitializer;
    private final QuoteInitializeManager manager;
    private final MonitorTask monitorTask;
    private final AppConfig appConfig;

    public static final Long ORG_ID_VALUE = 9001L;

    public QuoteWsApplication(BrokerInfoTask brokerInfoTask,
                              ConfigCenter configCenter,
                              WebSocketConfig webSocketConfig,
                              WebSocketInitializer webSocketInitializer,
                              QuoteInitializeManager manager,
                              MonitorTask monitorTask,
                              AppConfig appConfig) {
        this.brokerInfoTask = brokerInfoTask;
        this.configCenter = configCenter;
        this.webSocketConfig = webSocketConfig;
        this.webSocketInitializer = webSocketInitializer;
        this.manager = manager;
        this.monitorTask = monitorTask;
        this.appConfig = appConfig;
    }

    @PostConstruct
    public void startServer() {

        log.info("Config: [{}]", configCenter);
        // 用于处理订阅相关的操作
        manager.start();
        // 先获取quote-engine路由信息
        // 如果首次获取失败时，订阅时拿不到路由信息，按订阅失败处理
        manager.init();
        log.info("QuoteInitialManagerThread started");
        // 获取币对信息和broker信息,构建listener
        brokerInfoTask.init();
        manager.refreshSubscription();

        // start server 临时启动位置
        new WebSocketServer(webSocketConfig, webSocketInitializer).start();
        log.info("Server start!");
    }

    @PreDestroy
    public void shutdownHook() {
        manager.markShutdown();
        monitorTask.shutdownHook();
        manager.shutdown();
    }

    public static void main(String args[]) {
        SpringApplication.run(QuoteWsApplication.class, args);
    }

}
