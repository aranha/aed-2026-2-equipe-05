package br.pucminas.aed.credito.service;

import java.math.BigDecimal;
import java.time.OffsetDateTime;
import java.util.List;
import java.util.Optional;
import lombok.RequiredArgsConstructor;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Repository;

@Repository
@RequiredArgsConstructor
public class LimiteCreditoRepository {
    private final JdbcTemplate bancoDeDados;

    public boolean reservar(String clienteId, BigDecimal valor, OffsetDateTime atualizadoEm) {
        int linhas = bancoDeDados.update("""
                update limite_credito
                   set limite_disponivel = limite_disponivel - ?,
                       atualizado_em = ?
                 where cliente_id = ?
                   and limite_disponivel >= ?
                """, valor, atualizadoEm, clienteId, valor);
        return linhas == 1;
    }

    public boolean devolver(String clienteId, BigDecimal valor, OffsetDateTime atualizadoEm) {
        int linhas = bancoDeDados.update("""
                update limite_credito
                   set limite_disponivel = limite_disponivel + ?,
                       atualizado_em = ?
                 where cliente_id = ?
                   and limite_disponivel + ? <= limite_total
                """, valor, atualizadoEm, clienteId, valor);
        return linhas == 1;
    }

    public Optional<BigDecimal> consultarLimiteDisponivel(String clienteId) {
        List<BigDecimal> resultados = bancoDeDados.query(
                "select limite_disponivel from limite_credito where cliente_id = ?",
                (resultado, linha) -> resultado.getBigDecimal("limite_disponivel"), clienteId);
        return resultados.stream().findFirst();
    }

    public boolean existe(String clienteId) {
        Integer quantidade = bancoDeDados.queryForObject(
                "select count(*) from limite_credito where cliente_id = ?", Integer.class, clienteId);
        return quantidade != null && quantidade > 0;
    }
}
