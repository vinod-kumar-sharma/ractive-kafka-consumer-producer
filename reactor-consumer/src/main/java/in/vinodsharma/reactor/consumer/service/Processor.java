package in.vinodsharma.reactor.consumer.service;

import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import reactor.kafka.receiver.ReceiverRecord;

import java.util.List;
import java.util.Objects;
import java.util.Random;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

@Slf4j
@Service
public class Processor {

    private final ExecutorService executorService;

    public Processor(@Value("${spring.kafka.consumer.properties.max.poll.records}") Integer maxPollRecords) {
        this.executorService = Executors.newFixedThreadPool(maxPollRecords);
    }

    public void process(List<ReceiverRecord<String, String>> consumerRecords) {
        if (Objects.nonNull(consumerRecords) && !consumerRecords.isEmpty()) {
            consumerRecords.stream().parallel().forEach(record -> CompletableFuture.runAsync(() -> {
                try {
                    TimeUnit.SECONDS.sleep(new Random().nextInt(10));
                    record.receiverOffset().acknowledge();
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }, executorService).join());
        }
        long heapMaxSize = Runtime.getRuntime().maxMemory();
        long heapFreeSize = Runtime.getRuntime().freeMemory();
        log.info("HeapFreeSize  : Mb{}",heapFreeSize/1024);
        log.info("HeapMaxSize   : Mb{}",heapMaxSize/1024);
        log.info("Batch Size    :   {}",consumerRecords.size());
    }

    public void processGeneric(List<String> events) {
        //log.info("Consumed {}", events);
    }
}
