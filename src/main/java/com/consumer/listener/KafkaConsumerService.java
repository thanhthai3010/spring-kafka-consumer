package com.consumer.listener;

import com.consumer.model.UserModel;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

@Service
public class KafkaConsumerService {
    private static final Logger LOGGER = LoggerFactory.getLogger( KafkaConsumerService.class );

    @KafkaListener(topics = "first_topic", groupId = "first_topic_consumer", containerFactory = "kafkaListenerContainerFactory")
    public void receive(ConsumerRecord<String, String> record) {
        LOGGER.warn( "Received Message: " + record.value() + " - from partition: " + record.partition()
                + " - offset: " + record.offset() );

        // Commit offset - mark this message is processed and no-longer in message queue.
        // ack.acknowledge();
    }

    @KafkaListener(topics = "user_topic", groupId = "user-consumer-group", containerFactory = "userKafkaListenerContainerFactory")
    public void userReceiver(ConsumerRecord<String, UserModel> record) {
        LOGGER.warn( "Received Message: " + record.value() + " - from partition: " + record.partition()
                + " - offset: " + record.offset() );
    }
}
