package br.pucminas.aed.risco.service;

import br.pucminas.aed.risco.domain.CreditoSolicitadoEvent;
import lombok.RequiredArgsConstructor;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Repository;

@Repository
@RequiredArgsConstructor
public class AnaliseCreditoRepository {
    private final JdbcTemplate bancoDeDados;

    public void criar(CreditoSolicitadoEvent evento) {
        bancoDeDados.update("""
                insert into analise_credito
                    (solicitacao_id, cliente_id, valor_solicitado, status, solicitada_em)
                values (?, ?, ?, ?, ?)
                """, evento.getSolicitacaoId(), evento.getClienteId(), evento.getValorSolicitado(),
                "PENDENTE", evento.getDataSolicitacao());
    }

    public int contar() {
        return bancoDeDados.queryForObject("select count(*) from analise_credito", Integer.class);
    }

    public void excluirTodos() { bancoDeDados.update("delete from analise_credito"); }
}
