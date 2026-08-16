package com.aed20262equipe05.credito;

import com.aed20262equipe05.credito.domain.CreditoSolicitadoEvent;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.databind.json.JsonMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import java.util.Map;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.kafka.KafkaProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.kafka.support.serializer.JsonSerializer;

@Configuration
public class CreditoConfig {
    @Bean
    public NewTopic topicoDeCreditoSolicitado(
            @Value("${app.kafka.topico.credito-solicitado}") String topico) {
        return new NewTopic(topico, 3, (short) 1);
    }

    @Bean
    public ObjectMapper objectMapperDosEventos() {
        return JsonMapper.builder()
                .addModule(new JavaTimeModule())
                .disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS)
                .build();
    }

    @Bean
    public KafkaTemplate<String, CreditoSolicitadoEvent> clienteDoBroker(
            KafkaProperties propriedades, ObjectMapper objectMapperDosEventos) {
        var serializadorDoValor = new JsonSerializer<CreditoSolicitadoEvent>(objectMapperDosEventos);
        Map<String, Object> configuracao = propriedades.buildProducerProperties(null);
        configuracao.remove(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG);
        configuracao.remove(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG);
        ProducerFactory<String, CreditoSolicitadoEvent> fabrica =
                new DefaultKafkaProducerFactory<>(
                        configuracao, new StringSerializer(), serializadorDoValor);
        return new KafkaTemplate<>(fabrica);
    }
}
