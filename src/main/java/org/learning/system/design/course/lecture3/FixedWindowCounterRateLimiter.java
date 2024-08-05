package org.learning.system.design.course.lecture3;

import lombok.Getter;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

public class FixedWindowCounterRateLimiter implements RateLimiter {

    private static final Long WINDOW_SIZE = TimeUnit.SECONDS.toMillis(1L);
    private static final Integer CAPACITY = 5;

    private final Object lock = new Object();

    @Getter
    private final AtomicInteger count;

    private Long latestResetTimestamp;

    public FixedWindowCounterRateLimiter() {
        this.count = new AtomicInteger(0);
        this.latestResetTimestamp = System.currentTimeMillis();
    }

    @Override
    public boolean tryConsume() {
        synchronized (lock) {
            var now = System.currentTimeMillis();
            if (now - latestResetTimestamp > WINDOW_SIZE) {
                count.set(0);
                latestResetTimestamp = now;
            }
        }

        return this.count.incrementAndGet() <= CAPACITY;
    }
}
