package org.learning.system.design.course.lecture3;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class FixedWindowCounterRateLimiterTest {

    private final ExecutorService executorService = Executors.newFixedThreadPool(10);

    @ParameterizedTest
    @ValueSource(ints = {1, 2, 3, 4, 5})
    void tryConsume_parallelRequests_limitNotExceeded(Integer numberOfParallelRequests) throws ExecutionException, InterruptedException {
        var rateLimiter = new FixedWindowCounterRateLimiter();

        List<CompletableFuture<Boolean>> rateLimitingFutures = new ArrayList<>();
        for (var i = 0; i < numberOfParallelRequests; i++) {
            var rateLimitingFuture = CompletableFuture.supplyAsync(rateLimiter::tryConsume, executorService);
            rateLimitingFutures.add(rateLimitingFuture);
        }

        CompletableFuture.allOf(rateLimitingFutures.toArray(new CompletableFuture[0])).join();

        for (var rateLimitingFuture: rateLimitingFutures) {
            assertTrue(rateLimitingFuture.get());
        }

        assertEquals(numberOfParallelRequests, rateLimiter.getCount().get());
    }

    @Test
    void tryConsume_limitExceeded() throws ExecutionException, InterruptedException {
        var rateLimiter = new FixedWindowCounterRateLimiter();

        var numberOfParallelRequests = 6;
        List<CompletableFuture<Boolean>> rateLimitingFutures = new ArrayList<>();
        for (var i = 0; i < numberOfParallelRequests; i++) {
            var rateLimitingFuture = CompletableFuture.supplyAsync(rateLimiter::tryConsume, executorService);
            rateLimitingFutures.add(rateLimitingFuture);
        }

        CompletableFuture.allOf(rateLimitingFutures.toArray(new CompletableFuture[0])).join();

        assertEquals(numberOfParallelRequests, rateLimiter.getCount().get());

        var acceptedCount = 0;
        var rejectedCount = 0;
        for (var i = 0; i < numberOfParallelRequests; i++) {
            if (rateLimitingFutures.get(i).get()) {
                acceptedCount++;
            } else {
                rejectedCount++;
            }
        }

        assertEquals(5, acceptedCount);
        assertEquals(1, rejectedCount);
    }

    @Test
    void tryConsume_requestsWithDelay_limitNotExceeded() throws ExecutionException, InterruptedException {
        var rateLimiter = new FixedWindowCounterRateLimiter();

        var numberOfParallelRequests = 5;
        List<CompletableFuture<Boolean>> rateLimitingFutures = new ArrayList<>();
        for (var i = 0; i < numberOfParallelRequests; i++) {
            var rateLimitingFuture = CompletableFuture.supplyAsync(rateLimiter::tryConsume, executorService);
            rateLimitingFutures.add(rateLimitingFuture);
        }

        CompletableFuture.allOf(rateLimitingFutures.toArray(new CompletableFuture[0])).join();

        assertEquals(numberOfParallelRequests, rateLimiter.getCount().get());

        for (var i = 0; i < numberOfParallelRequests - 1; i++) {
            assertTrue(rateLimitingFutures.get(i).get());
        }

        // repeat 5 parallel request in a second
        Thread.sleep(TimeUnit.SECONDS.toMillis(1L));
        rateLimitingFutures.clear();
        for (var i = 0; i < numberOfParallelRequests; i++) {
            var rateLimitingFuture = CompletableFuture.supplyAsync(rateLimiter::tryConsume, executorService);
            rateLimitingFutures.add(rateLimitingFuture);
        }

        CompletableFuture.allOf(rateLimitingFutures.toArray(new CompletableFuture[0])).join();

        assertEquals(numberOfParallelRequests, rateLimiter.getCount().get());

        for (var i = 0; i < numberOfParallelRequests - 1; i++) {
            assertTrue(rateLimitingFutures.get(i).get());
        }
    }
}
