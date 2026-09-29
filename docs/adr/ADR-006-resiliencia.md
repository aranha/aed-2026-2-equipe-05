# ADR-006 — Retentativa, DLQ e falha da compensação

## Status
Aceita · 2026-09-29 · Equipe 05

## Contexto
O Kafka entrega cada evento pelo menos uma vez, e não há ordem garantida entre tópicos diferentes. Um consumidor que falha precisa decidir se tenta de novo, por quanto tempo e para onde vai o evento quando desiste. No domínio da [ADR-002](ADR-002-dominio.md), decidir errado tem dois custos: o **duplo desembolso**, quando uma retentativa repete um efeito financeiro, e o **limite preso**, quando um evento é descartado e a reserva fica sem desfecho.

O ponto de partida no código:

- O `servico-risco` consome `credito.solicitacao.solicitada.v1` com dois grupos (`risco-credito-v1` e `risco-fluxo-creditos-v1`) e deduplica a análise pelo `ce_id` (`EventoProcessadoRepository`).
- O `servico-credito` consumia `credito.elegibilidade.aprovada.v1`, `credito.proposta.recusada.v1` e `credito.proposta.expirada.v1` sem tratador de falha próprio. Valia o padrão do Spring Kafka 3.2: `DefaultErrorHandler` com `FixedBackOff(0, 9)`, isto é, 10 execuções seguidas, sem espera, e depois o evento era registrado no log e pulado.
- O Core Bancário e o desembolso não existem neste recorte. `ElegibilidadeAprovada`, `PropostaDeCreditoRecusada` e `PropostaDeCreditoExpirada` são publicados à mão pela Kafka UI.

A equipe tem sete integrantes e cada parte tem um dono: a análise e o caminho de falha no `servico-risco`, a reserva e a compensação no `servico-credito`, a documentação de arquitetura. Os dois serviços têm tabelas e deploy próprios e só se conhecem pelos tópicos; no ambiente padrão, as tabelas ficam no mesmo banco PostgreSQL (`BANCO_URL`).

## Decisão

### 1. Retentativa limitada e DLQ no `servico-risco`

Implementada em `RiscoConfig` (`tratadorDeFalhaDoConsumidor`) e configurável em `app.kafka.retentativa.*`:

- **Falha transitória** (por exemplo, banco indisponível): 4 retentativas, 5 execuções no total, com espera exponencial de 0,5 s, 1 s, 2 s e 4 s (`ExponentialBackOffWithMaxRetries`, inicial de 500 ms, multiplicador 2, máximo de 5 s). A soma das esperas é 7,5 s.
- **Falha permanente** (`IllegalArgumentException`, como `ce_id` ausente; `DataIntegrityViolationException`; payload que não desserializa): nenhuma retentativa, porque repetir produz o mesmo erro. A lista está em `CabecalhosDeFalhaFunction.FALHAS_PERMANENTES`.
- Esgotadas as tentativas, o `DeadLetterPublishingRecoverer` publica em `<tópico>.dlq` (`credito.solicitacao.solicitada.v1.dlq` e as DLQs dos dois eventos da saga) com a mesma chave, os cabeçalhos `ce_*` originais, o motivo e a origem nos cabeçalhos `kafka_dlt-exception-*` e `kafka_dlt-original-*`, e a classificação da falha no cabeçalho `classificacao` (`PERMANENTE` ou `TRANSITORIA`). O offset do registro recuperado é confirmado (`setCommitRecovered(true)`).

**Por que quatro.** A retentativa é bloqueante: enquanto um registro retenta, a partição espera, para manter a ordem por `solicitacaoId`. Quatro retentativas cobrem uma indisponibilidade curta com 7,5 s de espera. Esse número é só a espera. Com o banco fora do ar, cada execução ainda aguarda o timeout de conexão do Hikari (30 s, padrão), e o registro leva cerca de 2 min 40 s para chegar à DLQ (medido com o passo a passo do README). O `max.poll.interval.ms` (300.000 ms, padrão, não sobrescrito) não se compara com esse total: cada tentativa termina com um `seek` e é refeita num novo `poll`, então o limite vale para uma execução mais uma espera, cerca de 34 s no pior caso. Mais tentativas aumentariam o tempo com a partição parada sem mudar o desfecho de uma indisponibilidade longa, que é a DLQ.

### 2. Reprocessamento manual e idempotente

A DLQ é reprocessada sob demanda, pelo operador, depois de corrigida a causa. Com `app.kafka.reprocessamento-dlq.habilitado=true`, o `servico-risco` lê cada uma das suas DLQs uma vez na inicialização, com o grupo `risco-reprocessamento-dlq-v1`, até o fim que ela tinha no início, e republica no tópico original cada registro `TRANSITORIA`, com a mesma chave e os mesmos `ce_*`, sem os `kafka_dlt-*` e sem a `classificacao`. Registros `PERMANENTE` ficam na DLQ, porque republicá-los só repetiria o erro. A idempotência vem do dedup por `ce_id`. O que falhar de novo volta para a DLQ e fica para a próxima execução (`ReprocessamentoDlqService`, `ReprocessamentoDlqTest`).

**Quem olha e com que frequência.** O dono de cada serviço olha as DLQs dele: o de `servico-risco` as três do `servico-risco`, o de `servico-credito` as três do `servico-credito`. A conferência é diária, junto com a consulta de reservas `RESERVADA` sem desfecho (seção 7 do `arquitetura.md`), e também depois de qualquer incidente de banco ou de broker. As DLQs da compensação vêm primeiro, porque cada registro nelas é limite de cliente preso. Não há plantão nem alerta: é a frequência que uma equipe sem operação contínua consegue sustentar, e o custo está nas consequências aceitas.

### 3. Retentativa contra o Core Bancário

Não implementada neste recorte. A regra vale quando o desembolso existir:

- **Nenhuma retentativa sem chave de idempotência.** Se a primeira chamada liquidou e só a resposta se perdeu, uma retentativa sem chave transfere de novo: é o duplo desembolso. A chave é o `solicitacaoId`, porque cada solicitação libera crédito uma única vez.
- **Timeout é resultado desconhecido, não falha.** Antes de uma nova tentativa, o Core Bancário é consultado pela chave, e só se retenta o que comprovadamente não liquidou.
- **No máximo 3 tentativas.** Com a chave, a correção não depende do número, que passa a limitar só o tempo com a partição parada. São menos que as 5 execuções do `servico-risco` porque cada tentativa é uma chamada a um sistema externo, precedida de consulta, e uma falha que persiste por três tentativas indica indisponibilidade do Core, que não se resolve em segundos. Esgotadas, o evento vai para a DLQ e o desfecho é conciliado manualmente pela chave.

### 4. Falha da compensação

**Antes desta decisão.** Os listeners do `servico-credito` não tinham tratador próprio: uma compensação que lançava exceção era executada 10 vezes seguidas e pulada, com erro no log e sem DLQ. Dois casos conhecidos:

- **Recusa antes da reserva.** `CompensacaoReservaService.cancelar` não encontra a reserva e lança `IllegalStateException`. O evento era pulado. Se `ElegibilidadeAprovada` chegasse depois, a reserva seria criada e nunca compensada: o limite ficava preso.
- **Recusa e expiração para a mesma solicitação.** A expiração tem outro `ce_id` e não era reconhecida como já processada, porque a busca é por `evento_origem_id`. A devolução falhava pela condição `limite_disponivel + valor <= limite_total` (`IllegalStateException`) ou, se o cliente tivesse outra reserva aberta, o insert violava o `unique` de `cancelamento_reserva.solicitacao_id` e a transação desfazia a devolução. O limite ficava correto nos dois casos, mas o evento era executado 10 vezes e pulado com erro no log.

**Decisão.** O `servico-credito` adota a política do `servico-risco` (`CreditoConfig`, `FalhaDaSagaIntegracaoTest`):

- recusa ou expiração que chega antes da reserva é **falha transitória**: retenta e, esgotadas as tentativas, vai para a DLQ com `classificacao=TRANSITORIA`;
- cliente sem limite cadastrado ou com limite insuficiente é **falha permanente** (`IllegalArgumentException`): vai direto para a DLQ;
- um segundo gatilho para uma solicitação já compensada é ignorado sem erro: a reserva só passa de `RESERVADA` para `CANCELADA` uma vez (`ReservaLimiteRepository.marcarCancelada`), então o limite não é devolvido de novo e nenhum evento é publicado. Essa mudança de status é estado derivado, para a consulta; o fato que registra o desfazer é o `ReservaDeLimiteCancelada` publicado e a linha nova em `cancelamento_reserva`, e a reserva original não é apagada. A reentrega do mesmo gatilho (mesmo `ce_id`) reusa o cancelamento registrado e publica `ReservaDeLimiteCancelada` de novo;
- cada tópico consumido tem sua DLQ `<tópico>.dlq`.

O `servico-credito` ainda não tem reprocessador. Até lá, reprocessar uma compensação que está na DLQ é republicar o registro no tópico original pela Kafka UI, com a mesma chave e os mesmos `ce_*`. É seguro porque a compensação é idempotente pelo `ce_id` e pelo status da reserva.

### 5. Saga coreografada

A saga é coreografada: cada serviço reage aos eventos dos outros e nenhum componente comanda a sequência. Hoje ela tem um passo reversível (a reserva) e uma compensação, e os três eventos que a movem vêm de fora do código. Um orquestrador exigiria um serviço novo, com dono, para coordenar passos que ainda não existem, e concentraria num só lugar um fluxo que a equipe dividiu por serviço, o que a ADR-002 já recusou ao descartar o processo centralizado. A orquestração passa a valer quando contrato e desembolso entrarem no fluxo: esperas longas por ação humana e uma chamada ao Core Bancário com resultado possivelmente desconhecido pedem um componente que conheça o estado da saga e controle prazos.

## Alternativas consideradas

- **Retentar sem limite.** Descartada: uma falha permanente pararia a partição para sempre, e com ela todas as solicitações daquela partição.
- **Retentativa não bloqueante, com tópicos de retentativa (`@RetryableTopic`).** Descartada: libera a partição, mas o registro que falhou passa a ser processado depois dos seguintes da mesma chave, e a ordem por `solicitacaoId` se perde.
- **Sem DLQ, pulando o evento depois das tentativas** (o padrão do Spring Kafka, em vigor hoje no `servico-credito`). Descartada: o evento some sem um registro reprocessável e, na compensação, isso é limite preso.
- **Reprocessamento automático e contínuo da DLQ.** Descartada: um registro com falha permanente voltaria para a DLQ e seria reprocessado de novo, em loop.
- **Retentar o desembolso sem chave de idempotência.** Descartada: é o cenário do duplo desembolso.
- **Orquestrar a saga agora.** Descartada pelos motivos da decisão 5.

## Consequências aceitas

- **Partição parada durante a retentativa.** Enquanto um registro retenta, os seguintes da mesma partição esperam: 7,5 s de espera mais o tempo das 5 execuções, cerca de 2 min 40 s com o banco fora do ar.
- **Limite preso enquanto a compensação estiver na DLQ.** Uma recusa que chega antes da reserva e esgota as tentativas só compensa quando alguém reprocessar a DLQ, e no `servico-credito` esse reprocessamento ainda é manual, pela Kafka UI.
- **Reprocessamento depende de uma pessoa**, e a DLQ tem a retenção padrão do broker (`retention.ms` de 604.800.000, 7 dias). O que não for reprocessado nesse prazo se perde.
- **Uma cópia na DLQ por grupo.** Uma falha comum aos dois grupos do `servico-risco` gera dois registros na DLQ. O reprocessamento republica a origem uma vez.
- **Sem visão central da saga.** Nenhum componente conhece a sequência inteira; o desfecho de uma solicitação se reconstrói pelo estado de cada serviço (`analise_credito`, `reserva_limite`, `cancelamento_reserva`).
- **Regra do Core Bancário não testada.** A decisão 3 só passa a valer com o desembolso implementado e um registro local que responda se o desembolso daquela chave já saiu.
