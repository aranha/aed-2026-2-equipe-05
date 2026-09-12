package br.pucminas.aed.credito.service;

import br.pucminas.aed.credito.domain.LimiteDeCreditoReservadoEvent;
import lombok.RequiredArgsConstructor;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Repository;

@Repository
@RequiredArgsConstructor
public class ReservaLimiteRepository {
    private final JdbcTemplate bancoDeDados;

    public boolean existePorSolicitacaoId(String solicitacaoId) {
        Integer quantidade = bancoDeDados.queryForObject(
                "select count(*) from reserva_limite where solicitacao_id = ?",
                Integer.class, solicitacaoId);
        return quantidade != null && quantidade > 0;
    }

    public void criar(String eventoOrigemId, LimiteDeCreditoReservadoEvent evento) {
        bancoDeDados.update("""
                insert into reserva_limite
                    (solicitacao_id, evento_origem_id, evento_reserva_id, cliente_id,
                     valor_reservado, limite_disponivel_apos, status, reservada_em)
                values (?, ?, ?, ?, ?, ?, ?, ?)
                """, evento.getSolicitacaoId(), eventoOrigemId, evento.getEventoId(),
                evento.getClienteId(), evento.getValorReservado(), evento.getLimiteDisponivel(),
                "RESERVADA", evento.getDataReserva());
    }
}
