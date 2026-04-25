package com.aazim.kvstore.clock;

import java.util.concurrent.atomic.AtomicLong;

import org.springframework.stereotype.Component;

@Component
public class LamportClock {

    // Seeded from wall clock so existing log entries (stored as ms) remain valid after restart.
    private final AtomicLong counter = new AtomicLong(System.currentTimeMillis());

    public long tick() {
        return counter.incrementAndGet();
    }

    // Called on every incoming remote timestamp to ensure our clock is always ahead.
    public void update(long remoteTs) {
        counter.updateAndGet(local -> Math.max(local, remoteTs) + 1);
    }
}
