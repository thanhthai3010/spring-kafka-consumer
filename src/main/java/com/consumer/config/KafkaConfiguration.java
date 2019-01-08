package com.consumer.config;

import com.consumer.model.UserModel;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.support.serializer.JsonDeserializer;

import java.util.HashMap;
import java.util.Map;

@Configuration
@EnableKafka
public class KafkaConfiguration {

    @Value("${spring.kafka.bootstrap-servers}")
    private String bootstrapServers;

    @Value("${spring.kafka.consumer.group-id}")
    private String consumerGroupId;

    @Bean
    public ConsumerFactory<String, String> stringConsumerFactory() {
        Map<String, Object> props = consumerConfigBuilder(consumerGroupId);
        props.put( ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class );

        // Set this property, if auto commit should happen.
        // props.put( "enable.auto.commit", "false" );
        // props.put( ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest" );

        return new DefaultKafkaConsumerFactory<>( props );
    }

    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, String> kafkaListenerContainerFactory() {
        ConcurrentKafkaListenerContainerFactory<String, String> factory = new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory( stringConsumerFactory() );

        // Set this property, if auto commit should happen.
        // factory.getContainerProperties().setAckMode( AckMode.MANUAL_IMMEDIATE );

        return factory;
    }

    @Bean
    public ConsumerFactory<String, UserModel> userModelConsumerFactory() {
        Map<String, Object> props = consumerConfigBuilder("user-consumer-group");
        props.put( ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, UserModel.class );

        return new DefaultKafkaConsumerFactory<>( props, new StringDeserializer(), new JsonDeserializer<>( UserModel.class ) );
    }

    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, UserModel> userKafkaListenerContainerFactory() {
        ConcurrentKafkaListenerContainerFactory<String, UserModel> factory = new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory( userModelConsumerFactory() );

        return factory;
    }

    private Map<String, Object> consumerConfigBuilder(String consumerGroupId) {
        Map<String, Object> props = new HashMap<>();

        props.put( ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers );
        props.put( ConsumerConfig.GROUP_ID_CONFIG, consumerGroupId );
        props.put( ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class );
        return props;
    }
}
