package br.pucminas.aed.risco.service;

import java.util.List;
import lombok.RequiredArgsConstructor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

/**
 * Leva o desfecho da saga de reserva para a analise de credito, tornando o estado final
 * observavel em {@code analise_credito.status}: PENDENTE, RESERVADA ou CANCELADA.
 *
 * <p>Os eventos chegam por topicos diferentes, entao a ordem entre eles nao e garantida. As
 * transicoes so avancam (PENDENTE para RESERVADA, PENDENTE ou RESERVADA para CANCELADA): um
 * LimiteReservado atrasado nao desfaz um cancelamento ja registrado. Se a analise ainda nao
 * existe, o CreditoSolicitado ainda nao foi processado; a falha e transitoria e vai para a
 * retentativa.
 */
@Service
@RequiredArgsConstructor
public class AcompanhamentoReservaService {
    private static final Logger log = LoggerFactory.getLogger(AcompanhamentoReservaService.class);

    public static final String RESERVADA = "RESERVADA";
    public static final String CANCELADA = "CANCELADA";

    private final EventoProcessadoRepository eventoProcessadoRepository;
    private final AnaliseCreditoRepository analiseCreditoRepository;

    @Transactional
    public boolean registrarReserva(String eventoId, String solicitacaoId) {
        return registrar(eventoId, solicitacaoId, RESERVADA, List.of("PENDENTE"));
    }

    @Transactional
    public boolean registrarCancelamento(String eventoId, String solicitacaoId) {
        return registrar(eventoId, solicitacaoId, CANCELADA, List.of("PENDENTE", RESERVADA));
    }

    private boolean registrar(String eventoId, String solicitacaoId, String novoStatus,
                              List<String> statusDeOrigem) {
        if (!eventoProcessadoRepository.registrarSeNovo(eventoId)) {
            return false;
        }
        if (analiseCreditoRepository.atualizarStatus(solicitacaoId, novoStatus, statusDeOrigem)) {
            log.info("Analise de credito atualizada | solicitacaoId={} | status={}", solicitacaoId, novoStatus);
            return true;
        }
        var statusAtual = analiseCreditoRepository.buscarStatus(solicitacaoId).orElseThrow(
                () -> new IllegalStateException(
                        "analise de credito ainda nao registrada para a solicitacao " + solicitacaoId));
        log.info("Transicao ignorada | solicitacaoId={} | statusAtual={} | statusIgnorado={}",
                solicitacaoId, statusAtual, novoStatus);
        return false;
    }
}
