package io.bhex.broker.quote.job;

import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.ThreadPoolExecutor;

@Service
@Slf4j
public class MonitorTask {
    private final ScheduledExecutorService scheduledExecutorService;

    private final ExecutorService quoteServerThreadPool;

    private final ExecutorService rpcHealthCheckPool;

    @Autowired
    public MonitorTask(@Qualifier("scheduledTaskExecutor") ScheduledExecutorService scheduledExecutorService,
                       @Qualifier("quoteServerPool") ExecutorService quoteServerThreadPool,
                       @Qualifier("rpcHealthCheckExecutor") ExecutorService rpcHealthCheckPool) {
        this.scheduledExecutorService = scheduledExecutorService;
        this.quoteServerThreadPool = quoteServerThreadPool;
        this.rpcHealthCheckPool = rpcHealthCheckPool;
    }

    @Scheduled(initialDelay = 5000L, fixedRate = 5000L)
    public void threadPoolMonitor() {
        log.info("scheduled task cnt [{}], quotePool queue size [{}], rpcHealthExecutor [{}]",
            ((ScheduledThreadPoolExecutor) this.scheduledExecutorService).getActiveCount(),
            ((ThreadPoolExecutor) this.quoteServerThreadPool).getQueue().size(),
            ((ThreadPoolExecutor) this.rpcHealthCheckPool).getQueue().size());
    }

    public void shutdownHook() {
        log.info("Start shutdown thread pool");
        this.scheduledExecutorService.shutdownNow();
        this.quoteServerThreadPool.shutdownNow();
        this.rpcHealthCheckPool.shutdownNow();
        while (!this.scheduledExecutorService.isTerminated()
            || !this.quoteServerThreadPool.isTerminated()
            || !this.rpcHealthCheckPool.isTerminated()) {
            try {
                Thread.sleep(20L);
            } catch (InterruptedException e) {
                log.error(e.getMessage(), e);
                return;
            }
        }
        log.info("End shutdown thread pool");
    }
}
