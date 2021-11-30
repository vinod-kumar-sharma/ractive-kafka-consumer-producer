package in.vinodsharma.reactor.producer.service;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.reactive.ReactiveKafkaProducerTemplate;
import org.springframework.stereotype.Service;

import javax.annotation.PostConstruct;
import java.util.concurrent.TimeUnit;

@Service
public class ReactiveProducerService {

    private final Logger log = LoggerFactory.getLogger(ReactiveProducerService.class);
    private final ReactiveKafkaProducerTemplate<String, String> reactiveKafkaProducerTemplate;

    @Value(value = "${FAKE_PRODUCER_DTO_TOPIC}")
    private String topic;

    public ReactiveProducerService(ReactiveKafkaProducerTemplate<String, String> reactiveKafkaProducerTemplate) {
        this.reactiveKafkaProducerTemplate = reactiveKafkaProducerTemplate;
    }

    public void send(String fakeProducerDTO) {
        log.info("send to topic={}, {}={},", topic, String.class.getSimpleName(), fakeProducerDTO);
        reactiveKafkaProducerTemplate.send(topic, fakeProducerDTO)
                .doOnSuccess(senderResult -> log.info("sent {} offset : {}", fakeProducerDTO, senderResult.recordMetadata().offset()))
                .subscribe();
    }

    @PostConstruct
    public void sendOnStart() throws InterruptedException {
        int i = 0;
        while(true){
            i++;
            this.send(i+"messageJust as learning to design programs is important, so is the understanding of the correct format and usage of data. All programs use some form of data. To design programs which work correctly, a good understanding of how data is structured will be required.Just as learning to design programs is important, so is the understanding of the correct format and usage of data. All programs use some form of data. To design programs which work correctly, a good understanding of how data is structured will be required.Just as learning to design programs is important, so is the understanding of the correct format and usage of data. All programs use some form of data. To design programs which work correctly, a good understanding of how data is structured will be required.Just as learning to design programs is important, so is the understanding of the correct format and usage of data. All programs use some form of data. To design programs which work correctly, a good understanding of how data is structured will be required.Just as learning to design programs is important, so is the understanding of the correct format and usage of data. All programs use some form of data. To design programs which work correctly, a good understanding of how data is structured will be required.Just as learning to design programs is important, so is the understanding of the correct format and usage of data. All programs use some form of data. To design programs which work correctly, a good understanding of how data is structured will be required.Just as learning to design programs is important, so is the understanding of the correct format and usage of data. All programs use some form of data. To design programs which work correctly, a good understanding of how data is structured will be required.Just as learning to design programs is important, so is the understanding of the correct format and usage of data. All programs use some form of data. To design programs which work correctly, a good understanding of how data is structured will be required.");
            //TimeUnit.SECONDS.sleep(2);
        }
    }
}