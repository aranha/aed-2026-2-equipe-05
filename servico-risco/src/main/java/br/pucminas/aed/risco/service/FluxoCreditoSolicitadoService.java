package br.pucminas.aed.risco.service;

import br.pucminas.aed.risco.domain.CreditoSolicitadoEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.math.BigDecimal;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

@Service
public class FluxoCreditoSolicitadoService {

    private static final Logger log = LoggerFactory.getLogger(FluxoCreditoSolicitadoService.class);

    private static final ZoneOffset OFFSET_BRASILIA = ZoneOffset.of("-03:00");

    /**
     * Tamanho da janela, em minutos, para o fluxo de crédito solicitado.
     */
    private static final int TAMANHO_JANELA_MINUTOS = 5;

    private final ConcurrentMap<OffsetDateTime, AcumuladoJanela> acumuladosPorJanela = new ConcurrentHashMap<>();
    private final int maximoJanelasRetidas;

    public FluxoCreditoSolicitadoService(
            @Value("${app.fluxo-credito.maximo-janelas-retidas:288}") int maximoJanelasRetidas) {
        if (maximoJanelasRetidas <= 0) {
            throw new IllegalArgumentException("maximoJanelasRetidas deve ser positivo");
        }
        this.maximoJanelasRetidas = maximoJanelasRetidas;
    }

    /**
     * Guarda na memória o acumulado para uma janela definida em minutos e exibe no ‘log’.
     * Responde à pergunta de negócio: Qual foi o volume de crédito solicitado a cada janela de 5 minutos?
     *
     * @param evento Evento proveniente do broker.
     */
    public void agregar(CreditoSolicitadoEvent evento) {
        OffsetDateTime janela = alinharJanela(evento.getDataSolicitacao());

        AcumuladoJanela acumulado = acumuladosPorJanela.compute(janela, (chave, atual) -> {
            if (atual == null) {
                return new AcumuladoJanela(evento.getValorSolicitado(), 1);
            }

            return new AcumuladoJanela(
                    atual.totalSolicitado().add(evento.getValorSolicitado()),
                    atual.quantidadeSolicitacoes() + 1);
        });

        removerJanelasExcedentes();
        log.info("Fluxo de credito solicitado | janela={} | quantidade={} | totalSolicitado={}",
                janela, acumulado.quantidadeSolicitacoes(), acumulado.totalSolicitado());
    }

    /**
     * Verifica em qual janela o minuto da solicitação irá se "alinhar".
     * <p>
     * Por exemplo, considere uma janela de 5 minutos, uma solicitação no minuto 53 entraria na janela do minuto 50.
     * Porque 53 dividido por 5 é igual a 10 (aqui o resto não importa). Logo a janela do exemplo acima é a do minuto 50 porque 10 X 5 = 50.
     */
    private OffsetDateTime alinharJanela(OffsetDateTime dataSolicitacao) {
        OffsetDateTime horarioBrasilia = dataSolicitacao.withOffsetSameInstant(OFFSET_BRASILIA);
        int minutoAlinhado =
                (horarioBrasilia.getMinute() / TAMANHO_JANELA_MINUTOS) * TAMANHO_JANELA_MINUTOS;

        return horarioBrasilia.withMinute(minutoAlinhado).withSecond(0).withNano(0);
    }

    private void removerJanelasExcedentes() {
        int quantidadeExcedente = acumuladosPorJanela.size() - maximoJanelasRetidas;
        if (quantidadeExcedente <= 0) {
            return;
        }

        acumuladosPorJanela.keySet().stream()
                .sorted()
                .limit(quantidadeExcedente)
                .forEach(acumuladosPorJanela::remove);
    }

    private record AcumuladoJanela(BigDecimal totalSolicitado, int quantidadeSolicitacoes) {
    }

}
