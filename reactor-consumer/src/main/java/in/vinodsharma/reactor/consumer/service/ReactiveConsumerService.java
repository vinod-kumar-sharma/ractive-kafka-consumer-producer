package in.vinodsharma.reactor.consumer.service;

import in.vinodsharma.reactor.consumer.util.exception.ReceiverRecordException;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.CommandLineRunner;
import org.springframework.kafka.core.reactive.ReactiveKafkaConsumerTemplate;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Mono;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.stream.Collectors;

@Slf4j
@RequiredArgsConstructor
@Service
public class ReactiveConsumerService implements CommandLineRunner {

    @Value("${spring.kafka.consumer.properties.max.poll.records}")
    private Integer maxPollRecords;

    private final ReactiveKafkaConsumerTemplate<String, String> reactiveKafkaConsumerTemplate;
    private final Processor processor;

    private void startConsumer(String[] args) {
        reactiveKafkaConsumerTemplate
                .receive()
                .onBackpressureBuffer(maxPollRecords)
                .buffer(maxPollRecords)
                .doOnNext(processor::process)
                .map(records -> records.stream().map(ConsumerRecord::value).collect(Collectors.toList()))
                .doOnNext(processor::processGeneric)
                .retry()
                .doOnError(throwable -> log.error("something bad happened while consuming : {}", throwable))
                .onErrorResume(e -> {
                    ReceiverRecordException ex = (ReceiverRecordException) e.getCause();
                    System.out.println("Retries exhausted for " + ex.getRecord().value());
                    ex.getRecord().receiverOffset().acknowledge();
                    return Mono.empty();
                })
                .subscribe();
    }

    @Override
    public void run(String... args) {
        ExecutorService executorService = Executors.newFixedThreadPool(1);
        try {
            executorService.execute(() -> this.startConsumer(args));
        } catch (Exception e) {
            executorService.execute(() -> this.startConsumer(args));
        }
    }
}