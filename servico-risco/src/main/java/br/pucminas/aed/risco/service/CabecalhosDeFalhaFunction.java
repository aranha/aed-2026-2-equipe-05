package br.pucminas.aed.risco.service;

import com.fasterxml.jackson.core.JsonProcessingException;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.function.BiFunction;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.springframework.core.convert.ConversionException;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.kafka.support.serializer.DeserializationException;
import org.springframework.messaging.converter.MessageConversionException;
import org.springframework.messaging.handler.invocation.MethodArgumentResolutionException;

/**
 * Cabecalhos que o servico acrescenta a todo registro enviado para a DLQ, alem dos que o
 * Spring ja grava ({@code kafka_dlt-exception-message} e demais {@code kafka_dlt-*}).
 *
 * <p>{@code classificacao} diz se a falha e PERMANENTE (nunca vai dar certo sem corrigir o
 * registro) ou TRANSITORIA (dependencia fora do ar, ordem entre topicos). O reprocessamento so
 * reexecuta as transitorias. {@code reprocessamentos} conta quantas vezes o registro ja voltou
 * para a DLQ vindo do proprio reprocessamento, e e o que impede um laco.
 */
public class CabecalhosDeFalhaFunction implements BiFunction<ConsumerRecord<?, ?>, Exception, Headers> {

    public static final String CLASSIFICACAO = "classificacao";
    public static final String REPROCESSAMENTOS = "reprocessamentos";
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

    private final String sufixoDlq;

    public CabecalhosDeFalhaFunction(String sufixoDlq) {
        this.sufixoDlq = sufixoDlq;
    }

    @Override
    public Headers apply(ConsumerRecord<?, ?> registro, Exception falha) {
        var cabecalhos = new RecordHeaders();
        cabecalhos.add(CLASSIFICACAO, classificar(falha).getBytes(StandardCharsets.UTF_8));
        if (registro.topic().endsWith(sufixoDlq)) {
            int anteriores = lerInteiro(registro.headers().lastHeader(REPROCESSAMENTOS));
            cabecalhos.add(REPROCESSAMENTOS, String.valueOf(anteriores + 1).getBytes(StandardCharsets.UTF_8));
        }
        return cabecalhos;
    }

    public static String classificar(Throwable falha) {
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

    public static int lerInteiro(Header cabecalho) {
        if (cabecalho == null) {
            return 0;
        }
        try {
            return Integer.parseInt(new String(cabecalho.value(), StandardCharsets.UTF_8));
        } catch (NumberFormatException falha) {
            return 0;
        }
    }
}
