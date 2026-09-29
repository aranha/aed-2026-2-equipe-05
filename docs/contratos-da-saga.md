# Contratos dos eventos da saga de reserva de limite

Este documento estende o [contrato.md](contrato.md), que rege `credito.solicitacao.solicitada.v1`, aos cinco eventos da saga de reserva e compensação. As regras comuns valem para todos e não se repetem em cada seção.

## Regras comuns

- **Envelope:** CloudEvents 1.0 em modo binário. `ce_specversion`, `ce_id`, `ce_source`, `ce_type` e `ce_time` vão nos cabeçalhos; a carga vai no corpo, em JSON.
- **Tipo e tópico:** o nome do tópico é igual ao `ce_type`.
- **Identidade:** `eventoId` na carga é igual ao `ce_id`. É a chave de deduplicação dos consumidores.
- **Chave de partição:** `solicitacaoId` em todos os eventos. Todos os eventos da mesma solicitação caem na mesma partição do seu tópico. Não há ordem garantida entre tópicos diferentes; os consumidores tratam isso (ver [ADR-006](adr/ADR-006-resiliencia.md)).
- **Datas:** texto ISO-8601 com offset explícito, nunca epoch.
- **Valores monetários:** decimal com duas casas, em reais.
- **Compatibilidade:** **FULL**, pelas mesmas razões do `contrato.md`. Campo novo entra como opcional; campo obrigatório não é removido, renomeado nem muda de tipo ou de significado dentro da `v1`. Mudança de significado exige `v2` publicada em paralelo.

## Visão geral

| Evento | Tipo e tópico | Quem publica | Quem consome |
|---|---|---|---|
| ElegibilidadeAprovada | `credito.elegibilidade.aprovada.v1` | serviço de elegibilidade (fora deste recorte; hoje publicado à mão pela Kafka UI) | `servico-credito`, grupo `credito-reserva-limite-v1` |
| LimiteDeCreditoReservado | `credito.limite.reservado.v1` | `servico-credito`, `ce_source=/credito/limites` | `servico-risco`, grupo `risco-saga-reserva-v1` |
| PropostaDeCreditoRecusada | `credito.proposta.recusada.v1` | serviço de proposta (fora deste recorte; hoje publicado à mão) | `servico-credito`, grupo `credito-compensacao-reserva-v1` |
| PropostaDeCreditoExpirada | `credito.proposta.expirada.v1` | serviço de proposta (fora deste recorte; hoje publicado à mão) | `servico-credito`, grupo `credito-compensacao-reserva-v1` |
| ReservaDeLimiteCancelada | `credito.reserva-limite.cancelada.v1` | `servico-credito`, `ce_source=/credito/limites` | `servico-risco`, grupo `risco-saga-reserva-v1` |

## ElegibilidadeAprovada

Informa que a solicitação passou na avaliação de elegibilidade e qual valor foi aprovado. Não significa que o limite foi reservado nem que a proposta foi apresentada.

| Campo | Tipo | Obrigatório | Significado |
|---|---|---:|---|
| `eventoId` | string (UUID) | Sim | Identidade desta ocorrência do evento; igual ao `ce_id`. |
| `solicitacaoId` | string (UUID) | Sim | Solicitação de crédito avaliada, a mesma de `CreditoSolicitado`. |
| `clienteId` | string | Sim | Identificador interno do cliente cujo limite será reservado; não contém dado pessoal. |
| `valorAprovado` | decimal positivo | Sim | Valor que a avaliação autorizou e que será reservado do limite disponível. Pode ser menor que o valor solicitado. |
| `dataAprovacao` | ISO-8601 com offset | Sim | Instante em que a elegibilidade foi decidida. |

Uma elegibilidade para cliente sem limite cadastrado, ou com valor acima do limite disponível, é falha permanente: vai direto para a DLQ `credito.elegibilidade.aprovada.v1.dlq`.

```json
{
  "eventoId": "0b5f6c1e-7f7b-4d5a-9d0e-2a6d8f3c1b21",
  "solicitacaoId": "75ae6c98-2856-4717-9842-33a6f1f70953",
  "clienteId": "cli-ficticio-001",
  "valorAprovado": 3000.00,
  "dataAprovacao": "2026-09-27T10:00:00-03:00"
}
```

## LimiteDeCreditoReservado

Informa que o valor aprovado foi reservado do limite do cliente, antes da apresentação da proposta. É o efeito reversível que a compensação desfaz.

| Campo | Tipo | Obrigatório | Significado |
|---|---|---:|---|
| `eventoId` | string (UUID) | Sim | Identidade da reserva como fato; igual ao `ce_id`. É diferente do `eventoId` da elegibilidade que a originou. |
| `solicitacaoId` | string (UUID) | Sim | Solicitação cuja proposta passa a ter lastro. Existe no máximo uma reserva por solicitação. |
| `clienteId` | string | Sim | Cliente que teve o limite reduzido. |
| `valorReservado` | decimal positivo | Sim | Quanto saiu do limite disponível nesta reserva. |
| `limiteDisponivel` | decimal | Sim | Limite disponível do cliente **depois** da reserva, não antes. |
| `dataReserva` | ISO-8601 com offset | Sim | Instante em que a reserva foi gravada. |

Se `limiteDisponivel` passasse a representar o valor antes da reserva, o tipo e o esquema continuariam válidos e todo consumidor que o exibe como saldo mostraria o número errado. Essa mudança exige `credito.limite.reservado.v2`.

```json
{
  "eventoId": "5d8a9d0c-3b1e-4a7f-8c2d-6e1f0a9b8c77",
  "solicitacaoId": "75ae6c98-2856-4717-9842-33a6f1f70953",
  "clienteId": "cli-ficticio-001",
  "valorReservado": 3000.00,
  "limiteDisponivel": 7000.00,
  "dataReserva": "2026-09-27T10:00:05-03:00"
}
```

## PropostaDeCreditoRecusada

Informa que o cliente recusou explicitamente a proposta. É um dos dois gatilhos da compensação.

| Campo | Tipo | Obrigatório | Significado |
|---|---|---:|---|
| `eventoId` | string (UUID) | Sim | Identidade da recusa; igual ao `ce_id`. Vira o `eventoOrigemId` do cancelamento que ela provoca. |
| `solicitacaoId` | string (UUID) | Sim | Solicitação cuja proposta foi recusada. |
| `motivo` | string | Não | Motivo informado pelo cliente ou pelo canal, para auditoria. Não altera a compensação. |
| `dataRecusa` | ISO-8601 com offset | Sim | Instante em que a recusa aconteceu. |

```json
{
  "eventoId": "a1c2e3f4-1111-4222-8333-944455556666",
  "solicitacaoId": "75ae6c98-2856-4717-9842-33a6f1f70953",
  "motivo": "TAXA_ACIMA_DO_ESPERADO",
  "dataRecusa": "2026-09-27T10:05:00-03:00"
}
```

## PropostaDeCreditoExpirada

Informa que a proposta venceu sem resposta do cliente. É um gatilho diferente da recusa, com a mesma reação; o nome separado preserva na auditoria se o cliente recusou ou se o prazo venceu.

| Campo | Tipo | Obrigatório | Significado |
|---|---|---:|---|
| `eventoId` | string (UUID) | Sim | Identidade da expiração; igual ao `ce_id`. |
| `solicitacaoId` | string (UUID) | Sim | Solicitação cuja proposta expirou. |
| `dataExpiracao` | ISO-8601 com offset | Sim | Instante em que o prazo da proposta venceu, e não o instante em que a expiração foi detectada. |

Se a recusa e a expiração chegarem para a mesma solicitação, só a primeira compensa; a segunda é ignorada, porque a reserva já está `CANCELADA`.

```json
{
  "eventoId": "b7d8e9f0-2222-4333-8444-a55566667777",
  "solicitacaoId": "75ae6c98-2856-4717-9842-33a6f1f70953",
  "dataExpiracao": "2026-09-27T10:30:00-03:00"
}
```

## ReservaDeLimiteCancelada

É o evento de compensação: registra que a reserva foi desfeita e o limite devolvido. Não apaga a reserva nem a substitui; ela continua gravada com `status = CANCELADA`.

| Campo | Tipo | Obrigatório | Significado |
|---|---|---:|---|
| `eventoId` | string (UUID) | Sim | Identidade do cancelamento; igual ao `ce_id`. Uma reentrega do mesmo gatilho republica este mesmo `eventoId`. |
| `eventoOrigemId` | string (UUID) | Sim | `eventoId` da recusa ou da expiração que provocou o cancelamento. É a chave que impede devolver o limite duas vezes. |
| `solicitacaoId` | string (UUID) | Sim | Solicitação cuja reserva foi cancelada. |
| `clienteId` | string | Sim | Cliente que recebeu o limite de volta. |
| `valorDevolvido` | decimal positivo | Sim | Quanto voltou ao limite disponível; é igual ao `valorReservado` da reserva cancelada. |
| `limiteDisponivel` | decimal | Sim | Limite disponível do cliente **depois** da devolução. |
| `motivo` | string | Sim | `PROPOSTA_RECUSADA` ou `PROPOSTA_EXPIRADA`, conforme o gatilho. Novos valores podem ser acrescentados sem mudar o significado do campo. |
| `dataCancelamento` | ISO-8601 com offset | Sim | Instante em que a compensação foi gravada. |

```json
{
  "eventoId": "c3d4e5f6-3333-4444-8555-b66677778888",
  "eventoOrigemId": "a1c2e3f4-1111-4222-8333-944455556666",
  "solicitacaoId": "75ae6c98-2856-4717-9842-33a6f1f70953",
  "clienteId": "cli-ficticio-001",
  "valorDevolvido": 3000.00,
  "limiteDisponivel": 10000.00,
  "motivo": "PROPOSTA_RECUSADA",
  "dataCancelamento": "2026-09-27T10:05:01-03:00"
}
```
