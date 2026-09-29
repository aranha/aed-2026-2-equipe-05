package br.pucminas.aed.credito.service;

import br.pucminas.aed.credito.domain.ReservaDeLimiteCanceladaEvent;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.Optional;
import java.util.UUID;
import lombok.RequiredArgsConstructor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

@Service
@RequiredArgsConstructor
public class CompensacaoReservaService {
    private static final Logger log = LoggerFactory.getLogger(CompensacaoReservaService.class);
    private static final ZoneOffset OFFSET_BRASILIA = ZoneOffset.of("-03:00");

    private final LimiteCreditoRepository limiteCreditoRepository;
    private final ReservaLimiteRepository reservaLimiteRepository;
    private final CancelamentoReservaRepository cancelamentoReservaRepository;

    /**
     * Devolve o limite reservado e registra o cancelamento como fato novo.
     *
     * <p>Tres desfechos: o cancelamento ja existe para este evento (reentrega: devolve o mesmo
     * evento, para que a publicacao seja tentada de novo); a reserva ja foi encerrada por outro
     * evento da mesma solicitacao (recusa e expiracao: nada a fazer); ou a compensacao acontece
     * agora. Reserva inexistente e falha transitoria: o evento que a cria vem por outro topico e
     * pode ainda nao ter sido processado.
     */
    @Transactional
    public Optional<ReservaDeLimiteCanceladaEvent> cancelar(String eventoOrigemId,
                                                             String solicitacaoId,
                                                             String motivo) {
        var cancelamentoExistente = cancelamentoReservaRepository
                .buscarPorEventoOrigemId(eventoOrigemId);

        if (cancelamentoExistente.isPresent()) {
            return cancelamentoExistente;
        }

        var reserva = reservaLimiteRepository.buscarPorSolicitacaoId(solicitacaoId)
                .orElseThrow(() -> new IllegalStateException(
                        "reserva de limite nao encontrada para a solicitacao " + solicitacaoId));

        OffsetDateTime agora = OffsetDateTime.now(OFFSET_BRASILIA);
        if (!reservaLimiteRepository.marcarCancelada(solicitacaoId, agora)) {
            log.info("Compensacao ignorada | solicitacaoId={} | eventoOrigemId={} | status={}",
                    solicitacaoId, eventoOrigemId, reserva.status());
            return Optional.empty();
        }

        boolean devolvido = limiteCreditoRepository.devolver(
                reserva.clienteId(), reserva.valorReservado(), agora);

        if (!devolvido) {
            throw new IllegalStateException(
                    "nao foi possivel devolver o limite da solicitacao " + solicitacaoId);
        }

        var limiteDisponivel = limiteCreditoRepository
                .consultarLimiteDisponivel(reserva.clienteId())
                .orElseThrow();

        var evento = new ReservaDeLimiteCanceladaEvent(
                UUID.randomUUID().toString(),
                eventoOrigemId,
                solicitacaoId,
                reserva.clienteId(),
                reserva.valorReservado(),
                limiteDisponivel,
                motivo,
                agora);

        cancelamentoReservaRepository.criar(evento);
        return Optional.of(evento);
    }
}
