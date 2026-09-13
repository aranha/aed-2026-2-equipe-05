package br.pucminas.aed.credito.service;

import br.pucminas.aed.credito.domain.ElegibilidadeAprovadaEvent;
import br.pucminas.aed.credito.domain.LimiteDeCreditoReservadoEvent;
import java.math.BigDecimal;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.Optional;
import java.util.UUID;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

@Service
@RequiredArgsConstructor
public class ReservaLimiteService {
    private static final ZoneOffset OFFSET_BRASILIA = ZoneOffset.of("-03:00");

    private final LimiteCreditoRepository limiteCreditoRepository;
    private final ReservaLimiteRepository reservaLimiteRepository;

    @Transactional
    public Optional<LimiteDeCreditoReservadoEvent> reservar(String eventoOrigemId,
                                                             ElegibilidadeAprovadaEvent elegibilidade) {
        if (reservaLimiteRepository.existePorSolicitacaoId(elegibilidade.getSolicitacaoId())) {
            return Optional.empty();
        }

        OffsetDateTime agora = OffsetDateTime.now(OFFSET_BRASILIA);
        boolean reservado = limiteCreditoRepository.reservar(
                elegibilidade.getClienteId(), elegibilidade.getValorAprovado(), agora);

        if (!reservado) {
            if (!limiteCreditoRepository.existe(elegibilidade.getClienteId())) {
                throw new IllegalStateException(
                        "limite de credito nao cadastrado para o cliente " + elegibilidade.getClienteId());
            }
            throw new IllegalStateException(
                    "limite de credito insuficiente para a solicitacao " + elegibilidade.getSolicitacaoId());
        }

        BigDecimal limiteDisponivel = limiteCreditoRepository
                .consultarLimiteDisponivel(elegibilidade.getClienteId())
                .orElseThrow();

        var evento = new LimiteDeCreditoReservadoEvent(
                UUID.randomUUID().toString(),
                elegibilidade.getSolicitacaoId(),
                elegibilidade.getClienteId(),
                elegibilidade.getValorAprovado(),
                limiteDisponivel,
                agora);

        reservaLimiteRepository.criar(eventoOrigemId, evento);
        return Optional.of(evento);
    }
}
