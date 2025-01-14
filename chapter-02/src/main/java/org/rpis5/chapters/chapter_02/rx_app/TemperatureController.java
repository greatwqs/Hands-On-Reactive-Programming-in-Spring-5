package org.rpis5.chapters.chapter_02.rx_app;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.context.request.async.AsyncRequestTimeoutException;
import org.springframework.web.servlet.ModelAndView;
import org.springframework.web.servlet.mvc.method.annotation.SseEmitter;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;

@RestController
public class TemperatureController {
    private static final Logger log = LoggerFactory.getLogger(TemperatureController.class);

    private final TemperatureSensor temperatureSensor;

    public TemperatureController(TemperatureSensor temperatureSensor) {
        this.temperatureSensor = temperatureSensor;
    }

    @RequestMapping(value = "/temperature-stream", method = RequestMethod.GET)
    public SseEmitter events(HttpServletRequest request) {
        RxSeeEmitter emitter = new RxSeeEmitter();
        log.info("[{}] Rx SSE stream opened for client: {}",
                emitter.getSessionId(), request.getRemoteAddr());

        temperatureSensor.temperatureStream()
                .subscribe(emitter.getSubscriber());

        return emitter;
    }

    @ExceptionHandler(value = AsyncRequestTimeoutException.class)
    public ModelAndView handleTimeout(HttpServletResponse rsp) throws IOException {
        if (!rsp.isCommitted()) {
            rsp.sendError(HttpServletResponse.SC_SERVICE_UNAVAILABLE);
        }
        return new ModelAndView();
    }

}
