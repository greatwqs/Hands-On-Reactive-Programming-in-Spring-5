package org.rpis5.chapters.chapter_02.rx_app;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.web.servlet.mvc.method.annotation.SseEmitter;
import rx.Subscriber;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicInteger;

/***
 *
 * @author wang.qingsong
 * @created on 2023/2/15
 */
public class RxSeeEmitter extends SseEmitter {

    static final Logger log = LoggerFactory.getLogger(RxSeeEmitter.class);
    static final long SSE_SESSION_TIMEOUT = 30 * 60 * 1000L;
    private final static AtomicInteger sessionIdSequence = new AtomicInteger(0);
    private final int sessionId = sessionIdSequence.incrementAndGet();

    private final Subscriber<Temperature> subscriber = buildNewSubscriber();

    public RxSeeEmitter() {
        super(SSE_SESSION_TIMEOUT);

        super.onCompletion(() -> {
            log.info("[{}] SSE completed", sessionId);
            subscriber.unsubscribe();
        });

        super.onTimeout(() -> {
            log.info("[{}] SSE timeout", sessionId);
            subscriber.unsubscribe();
        });
    }

    Subscriber<Temperature> getSubscriber() {
        return subscriber;
    }

    int getSessionId() {
        return sessionId;
    }

    private Subscriber<Temperature> buildNewSubscriber() {
        return new Subscriber<Temperature>() {
            @Override
            public void onNext(Temperature temperature) {
                try {
                    RxSeeEmitter.this.send(temperature);
                    log.info("[{}] << {} ", sessionId, temperature.getValue());
                } catch (IOException e) {
                    log.warn("[{}] Can not send event to SSE, closing subscription, message: {}",
                            sessionId, e.getMessage());
                    unsubscribe();
                }
            }

            @Override
            public void onError(Throwable e) {
                log.warn("[{}] Received sensor error: {}", sessionId, e.getMessage());
            }

            @Override
            public void onCompleted() {
                log.warn("[{}] Stream completed", sessionId);
            }
        };
    }
}