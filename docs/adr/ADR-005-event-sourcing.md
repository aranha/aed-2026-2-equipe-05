# ADR-005 — Event Sourcing: o que guardamos como fato e o que guardamos como estado

## Status
Aceita · 2026-09-29 · Equipe 05

## Contexto

O log do Kafka é append-only: um fato publicado não é alterado nem apagado. Esse princípio
já governa a compensação da [ADR-002](ADR-002-dominio.md) — quando uma proposta é recusada,
a reserva não é removida, e sim desfeita por um fato novo, `ReservaDeLimiteCancelada`.

A pergunta que este ADR responde é outra, e ela aparece assim que alguém precisa saber
**em que passo está uma solicitação**: o estado do domínio deve ser *derivado* dos eventos,
reconstruído por replay a partir de um event store, ou deve ser *guardado* em tabelas que
os consumidores atualizam à medida que os eventos chegam?

Hoje o sistema guarda estado em tabelas:

| Tabela | Serviço | O que guarda |
|---|---|---|
| `analise_credito` | `servico-risco` | uma linha por solicitação, com `status` |
| `limite_credito` | `servico-credito` | o limite disponível de cada cliente |
| `reserva_limite` | `servico-credito` | a reserva, com `status` e `cancelada_em` |
| `cancelamento_reserva` | `servico-credito` | o cancelamento, indexado pelo evento de origem |
| `evento_processado` | `servico-risco` | os `ce_id` já consumidos, para deduplicação |

Nenhuma delas é um event store. O histórico dos fatos vive nos tópicos do Kafka; as tabelas
guardam o resultado de tê-los processado.

## Decisão

**Não adotamos Event Sourcing como mecanismo de persistência.** O estado do domínio é
guardado diretamente, e os eventos são o meio de comunicação entre serviços — não a fonte
da qual o estado é reconstruído.

O que mantemos do princípio append-only:

- **Fato nunca é alterado nem apagado.** A compensação publica `ReservaDeLimiteCancelada`
  e grava uma linha nova em `cancelamento_reserva`. A linha original de `reserva_limite`
  permanece, e o que muda nela é o `status`, que é projeção do desfecho — não o apagamento
  do fato de que a reserva existiu.
- **O evento é a unidade de idempotência.** A deduplicação é pelo `ce_id`, a identidade da
  ocorrência publicada, como a [ADR-002](ADR-002-dominio.md) justifica. É isso que torna
  o reprocessamento da DLQ seguro ([ADR-006](ADR-006-resiliencia.md)).
- **O tópico é reprocessável.** Reler `credito.solicitacao.solicitada.v1` com um grupo novo
  reconstrói a agregação por janela do zero, e o agregador deduplica por `ce_id` para que
  reentregas não contem duas vezes.

O que recusamos, e o registro está em [`docs/IA.md`](../IA.md): tornar a Solicitação de
Crédito um agregado event sourced, com event store append-only e projeção reconstruída por
replay. A pergunta que motivou a sugestão — *em que passo está esta solicitação* — é
respondida por dois campos de status, e o custo de um segundo modelo de persistência não se
paga contra isso.

## Alternativas consideradas

- **Event Sourcing completo da Solicitação de Crédito.** Um event store como fonte da
  verdade e projeções reconstruídas por replay. Recusada por três custos concretos: dois
  modelos de persistência para manter e evoluir; versionamento dos eventos gravados, que
  exige *upcasting* quando o esquema muda — e o `contrato.md` já prevê que ele vai mudar;
  e dado pessoal num log imutável, que conflita com a possibilidade de correção e remoção.
  O ganho seria responder perguntas sobre o passado que ainda não temos.

- **Event Sourcing apenas da reserva de limite.** Limitar o event store ao agregado que tem
  efeito reversível. Recusada porque o benefício — reconstruir o saldo do limite a partir
  dos fatos — já é obtido com `reserva_limite` e `cancelamento_reserva`, que juntas contam
  a mesma história, e porque um único agregado event sourced no meio de um sistema que não
  é paga o custo de ambos os modelos sem a coerência de nenhum.

- **CQRS com projeção persistida do agregador.** Gravar o volume por janela numa tabela em
  vez de mantê-lo em memória. **Não recusada — adiada.** Está nomeada como trabalho futuro
  na seção 8 do [`arquitetura.md`](../arquitetura.md), e é o que permitiria escalar o
  agregador para mais de uma instância.

## Consequências aceitas

- **O passado só responde o que as tabelas guardam.** Uma pergunta nova sobre o histórico —
  "quantas vezes este cliente teve reserva cancelada em agosto" — depende de o dado ter sido
  guardado na época. Num sistema event sourced, bastaria reprocessar. Aqui, não basta.

- **O Kafka não é arquivo.** Os tópicos têm a retenção padrão do broker, sete dias. Passado
  esse prazo, os fatos não estão mais lá para serem relidos, e o que sobrou é o que as
  tabelas guardaram. Tratar o log como fonte permanente seria confiar numa garantia que a
  configuração atual não dá.

- **O agregador de fluxo perde tudo num restart.** Ele mantém as janelas e o conjunto de
  `ce_id` em memória. Reiniciar o serviço zera os totais, e como o grupo já confirmou os
  offsets, recuperá-los exige reler o tópico com um grupo novo. É o preço de não ter
  projeção persistida, e está aceito enquanto a pergunta for operacional e não contábil.

- **`reserva_limite.status` é projeção, e projeção pode divergir.** Ele é atualizado pelo
  consumidor da compensação. Se esse consumidor falhar e o evento ficar na DLQ, a linha
  continua marcada `RESERVADA` enquanto o cancelamento já aconteceu no fato. A
  [ADR-006](ADR-006-resiliencia.md) trata essa janela; ela existe porque o estado é
  guardado, não derivado.
