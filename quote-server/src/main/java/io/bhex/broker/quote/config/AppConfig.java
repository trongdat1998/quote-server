package io.bhex.broker.quote.config;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import io.bhex.broker.quote.mq.Groups;
import io.bhex.broker.quote.mq.MQMarkPriceListener;
import io.bhex.ex.quote.IQuoteDataGRpcClientAdaptor;
import io.bhex.ex.quote.IQuoteGRpcClientAdaptor;
import io.bhex.exchange.lang.AddressConfig;
import io.netty.util.concurrent.DefaultThreadFactory;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.common.consumer.ConsumeFromWhere;
import org.apache.rocketmq.common.protocol.heartbeat.MessageModel;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.DefaultResourceLoader;
import org.springframework.core.io.ResourceLoader;
import reactor.core.scheduler.Scheduler;
import reactor.core.scheduler.Schedulers;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

@Configuration
@EnableConfigurationProperties
@Slf4j
public class AppConfig {

    @Value("${bhex.ws.port}")
    private Integer wsPort;
    @Value("${spring.application.name}")
    @Getter
    private String applicationName;

    @Bean("push-scheduler")
    public Scheduler pushScheduler() {
        return Schedulers.fromExecutor(new ThreadPoolExecutor(256, 512, 10, TimeUnit.SECONDS, new LinkedBlockingQueue<>(5000),
                new DefaultThreadFactory("push-scheduler"),
                new ThreadPoolExecutor.CallerRunsPolicy()));
        //return Schedulers.newParallel("push-schedulers", 256);
    }

    @Bean("bh-server")
    @ConfigurationProperties("bhex.bh-server")
    public AddressConfig bhGrpcServerConfig() {
        return new AddressConfig();
    }

    @Bean("quote-data-service")
    @ConfigurationProperties("bhex.data-service")
    public AddressConfig quoteDataService() {
        return new AddressConfig();
    }

    @Bean("gateway-server")
    @ConfigurationProperties("bhex.gateway")
    @ConditionalOnProperty(name = "bhex.proxy", havingValue = "true")
    public AddressConfig gatewayServerConfig() {
        return new AddressConfig();
    }

    @Bean
    @Autowired
    public DefaultMQPushConsumer markPriceConsumerGroup(MQMarkPriceListener mqMarkPriceListener) {
        DefaultMQPushConsumer consumerGroup = new DefaultMQPushConsumer(Groups.getMarkPriceConsumerGroupName());
        consumerGroup.setMessageModel(MessageModel.BROADCASTING);
        consumerGroup.setConsumeFromWhere(ConsumeFromWhere.CONSUME_FROM_LAST_OFFSET);
        consumerGroup.setConsumeThreadMax(1);
        consumerGroup.setConsumeThreadMin(1);
        consumerGroup.setAdjustThreadPoolNumsThreshold(1);
        consumerGroup.registerMessageListener(mqMarkPriceListener);
        return consumerGroup;
    }

    @Bean
    @Autowired
    public IQuoteGRpcClientAdaptor quoteGRpcClientAdaptor(GrpcRunner grpcRunner) {
        return grpcRunner::borrowDataServiceChannel;
    }

    @Bean
    @Autowired
    public IQuoteDataGRpcClientAdaptor quoteDataGRpcClientAdaptor(GrpcRunner grpcRunner) {
        return grpcRunner::borrowDataServiceChannel;
    }

    /**
     * 用来发送的行情数据的线程池
     *
     * @return scheduleExecutor
     */
    @Bean("scheduledTaskExecutor")
    public ScheduledExecutorService scheduledTaskExecutor() {
        ThreadFactory threadFactory = new ThreadFactoryBuilder()
            .setNameFormat("scheduled_pool %d")
            .build();
        return new ScheduledThreadPoolExecutor(20,
            threadFactory, new ThreadPoolExecutor.DiscardOldestPolicy() {
            @Override
            public void rejectedExecution(Runnable r, ThreadPoolExecutor e) {
                super.rejectedExecution(r, e);
                log.warn("scheduledTaskExecutor DiscardOldestPolicy");
            }
        });
    }

    @Bean("quoteServerPool")
    public ExecutorService executorService() {
        return new ThreadPoolExecutor(32, 64, 20,
            TimeUnit.SECONDS, new LinkedBlockingQueue<>(100000),
            new DefaultThreadFactory("quote-server-pool"),
            new ThreadPoolExecutor.CallerRunsPolicy());
    }

    @Bean("rpcHealthCheckExecutor")
    public ExecutorService rpcHealthCheckExecutor() {
        return new ThreadPoolExecutor(8, 32, 20,
            TimeUnit.SECONDS, new LinkedBlockingQueue<>(10000),
            new DefaultThreadFactory("rpc-health-pool"),
            new ThreadPoolExecutor.DiscardOldestPolicy() {
                @Override
                public void rejectedExecution(Runnable r, ThreadPoolExecutor e) {
                    super.rejectedExecution(r, e);
                    log.warn("Rpc health check thread pool is full, discard oldest task.");
                }
            });
    }

    @Bean
    public ResourceLoader createResourceLoader() {
        return new DefaultResourceLoader();
    }

}
