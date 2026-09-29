package br.pucminas.aed.risco.service;

import com.fasterxml.jackson.core.JsonProcessingException;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.function.BiFunction;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.springframework.core.convert.ConversionException;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.kafka.support.serializer.DeserializationException;
import org.springframework.messaging.converter.MessageConversionException;
import org.springframework.messaging.handler.invocation.MethodArgumentResolutionException;

/**
 * Cabecalho que o servico acrescenta a todo registro enviado para a DLQ, alem dos que o
 * Spring ja grava ({@code kafka_dlt-exception-message} e demais {@code kafka_dlt-*}).
 *
 * <p>{@code classificacao} diz se a falha e PERMANENTE (nunca vai dar certo sem corrigir o
 * registro) ou TRANSITORIA (dependencia fora do ar, ordem entre topicos). O
 * {@link ReprocessamentoDlqService} so republica as transitorias.
 */
public class CabecalhosDeFalhaFunction implements BiFunction<ConsumerRecord<?, ?>, Exception, Headers> {

    public static final String CLASSIFICACAO = "classificacao";
    public static final String PERMANENTE = "PERMANENTE";
    public static final String TRANSITORIA = "TRANSITORIA";

    public static final List<Class<? extends Exception>> FALHAS_PERMANENTES = List.of(
            IllegalArgumentException.class,
            DataIntegrityViolationException.class,
            JsonProcessingException.class,
            DeserializationException.class,
            MessageConversionException.class,
            ConversionException.class,
            MethodArgumentResolutionException.class,
            ClassCastException.class);

    @Override
    public Headers apply(ConsumerRecord<?, ?> registro, Exception falha) {
        return new RecordHeaders().add(CLASSIFICACAO, classificar(falha).getBytes(StandardCharsets.UTF_8));
    }

    private static String classificar(Throwable falha) {
        for (Throwable atual = falha; atual != null; atual = atual.getCause()) {
            for (Class<? extends Exception> permanente : FALHAS_PERMANENTES) {
                if (permanente.isInstance(atual)) {
                    return PERMANENTE;
                }
            }
            if (atual.getCause() == atual) {
                break;
            }
        }
        return TRANSITORIA;
    }
}
