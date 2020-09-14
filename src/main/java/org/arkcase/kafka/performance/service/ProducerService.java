package main.java.org.arkcase.kafka.performance.service;

import org.apache.avro.generic.GenericRecord;
import org.arkcase.kafka.performance.configuration.KafkaProducerConfiguration;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Service;
import org.springframework.util.concurrent.ListenableFuture;
import org.springframework.util.concurrent.ListenableFutureCallback;

/**
 * Created by Vladimir Cherepnalkovski <vladimir.cherepnalkovski@armedia.com> on Feb, 2020
 */
@Service
public class ProducerService
{
    @Value(value = "${spring.kafka.topic.generatedMessages}")
    private String generated_messages;

    private final KafkaProducerConfiguration kafkaProducerConfiguration;

    public ProducerService(KafkaProducerConfiguration kafkaProducerConfiguration)
    {
        this.kafkaProducerConfiguration = kafkaProducerConfiguration;
    }

    public void sendGeneratedMessage(GenericRecord event)
    {
        ListenableFuture<SendResult<String, GenericRecord>> future =
                kafkaProducerConfiguration.kafkaTemplate().send(generated_messages, event.get("userId").toString(), event);

        future.addCallback(new ListenableFutureCallback<SendResult<String, GenericRecord>>() {

            @Override
            public void onSuccess(SendResult<String, GenericRecord> result) {
             /* System.out.println("Sent message=[" + event.toString() +
                        "] with offset=[" + result.getRecordMetadata().offset() + "]");*/
            }
            @Override
            public void onFailure(Throwable ex) {
                System.out.println("Unable to send message=["
                        + event.toString() + "] due to : " + ex.getMessage());
            }
        });
    }

    public void insertPeople(GenericRecord event)
    {
        ListenableFuture<SendResult<String, GenericRecord>> future =
                kafkaProducerConfiguration.kafkaTemplate().send("peoples", event.get("id").toString(), event);

        future.addCallback(new ListenableFutureCallback<SendResult<String, GenericRecord>>() {

            @Override
            public void onSuccess(SendResult<String, GenericRecord> result) {
                System.out.println("Sent message=[" + event.toString() +
                        "] with offset=[" + result.getRecordMetadata().offset() + "]");
            }
            @Override
            public void onFailure(Throwable ex) {
                System.out.println("Unable to send message=["
                        + event.toString() + "] due to : " + ex.getMessage());
            }
        });
    }

    public void deleteUser(String userId)
    {
        ListenableFuture<SendResult<String, GenericRecord>> future =
                kafkaProducerConfiguration.kafkaTemplate().send(generated_messages, userId, null);

        future.addCallback(new ListenableFutureCallback<SendResult<String, GenericRecord>>() {

            @Override
            public void onSuccess(SendResult<String, GenericRecord> result) {
                System.out.println("Sent message=[" + "Delete user with id" + userId +
                        "] with offset=[" + result.getRecordMetadata().offset() + "]");
            }
            @Override
            public void onFailure(Throwable ex) {
                System.out.println("Unable to send message=["
                        + "Delete user with id" + userId + "] due to : " + ex.getMessage());
            }
        });
    }
}
