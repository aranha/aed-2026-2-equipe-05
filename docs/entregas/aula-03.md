# Aula 03 - Agregação por janela de tempo

## Por onde começar a leitura

1. [README do projeto](../../README.md), seção "Consumidor de fluxo por janela de tempo": como subir o ambiente e reproduzir a agregação com exemplos de `curl`.
2. [contrato.md](../contrato.md): o evento `credito.solicitacao.solicitada.v1` consumido pelo agregador, e a regra de compatibilidade FULL que permite os dois grupos de consumo lerem o mesmo tópico.
3. [FluxoCreditoSolicitadoListener](../../servico-risco/src/main/java/br/pucminas/aed/risco/controller/FluxoCreditoSolicitadoListener.java): consumidor com grupo próprio (`risco-fluxo-creditos-v1`), separado do consumidor de análise de crédito.
4. [FluxoCreditoSolicitadoService](../../servico-risco/src/main/java/br/pucminas/aed/risco/service/FluxoCreditoSolicitadoService.java): alinhamento da janela de 5 minutos pelo relógio de ocorrência (`dataSolicitacao`) e acumulação em memória.
5. [FluxoCreditoSolicitadoServiceTest](../../servico-risco/src/test/java/br/pucminas/aed/risco/service/FluxoCreditoSolicitadoServiceTest.java) e [FluxoCreditoSolicitadoTest](../../servico-risco/src/test/java/br/pucminas/aed/risco/FluxoCreditoSolicitadoTest.java): cobertura da normalização de janelas.
6. As perguntas de negócio respondidas abaixo: o que a agregação mede, por que o relógio de ocorrência foi escolhido, o que acontece com retardatários e o que muda num reprocessamento.

## Qual pergunta de negócio a agregação responde?

A agregação responde a pergunta: **qual foi o volume de crédito solicitado a cada janela fixa de 5 minutos?**

Para cada janela, o consumidor de fluxo calcula a quantidade de solicitações recebidas e a soma do campo `valorSolicitado`. Exemplo: em vez de responder o que aconteceu com uma solicitação específica, o fluxo mostra quanto crédito foi solicitado entre `12:00` e `12:04`, entre `12:05` e `12:09`, e assim por diante.

## Qual relógio foi escolhido, ocorrência ou chegada, e por quê?

Foi escolhido o relógio de **ocorrência** do evento, usando o campo `dataSolicitacao`. Essa escolha foi feita porque a pergunta de negócio está relacionada ao momento em que o cliente solicitou o crédito, e não ao momento em que o `servico-risco` recebeu a mensagem do Kafka.

Com isso, as janelas são alinhadas pelo horário do evento. Uma solicitação com `dataSolicitacao` em `2026-08-22T12:02:34-03:00`, por exemplo, entra na janela `2026-08-22T12:00:00-03:00`.

## O que acontece com um evento que chega atrasado?

No desenho atual, um evento atrasado ainda entra na janela correspondente ao seu horário de ocorrência enquanto essa janela estiver dentro do limite de retenção. Se um evento de `12:02` chegar quando o sistema já estiver processando eventos de `12:10`, ele atualiza a janela de `12:00`.

Como a agregação é mantida em memória e o resultado é exibido em log, não existe watermark. Para evitar crescimento ilimitado de memória, são mantidas por padrão as 288 janelas mais recentes. Um evento mais antigo do que essa retenção é processado e exibido no log, mas sua janela pode ser removida da memória quando o limite for reaplicado.

## Se o fluxo fosse reprocessado do começo amanhã, o resultado seria o mesmo?

Com o estado em memória vazio, o reprocessamento da mesma sequência de eventos reproduz os mesmos totais nas janelas retidas, porque a agregação usa o horário de ocorrência (`dataSolicitacao`) e os valores do próprio evento. A retenção deve ser considerada nessa comparação: janelas descartadas e seus IDs não ficam disponíveis para consultas ou deduplicação posterior.

O agregador deduplica por `eventoId` enquanto a janela correspondente permanece em memória: antes de somar uma solicitação, verifica se o ID está no Set de eventos agregados. Durante esse período, uma reentrega do Kafka com o mesmo `eventoId` não incrementa novamente a quantidade nem o `totalSolicitado`.

A deduplicação acompanha a retenção das janelas: cada janela mantém os IDs dos eventos agregados e, ao ser removida, seus IDs também são excluídos do Set de deduplicação. Assim, o Set armazena apenas IDs associados às janelas retidas, cujo limite padrão é 288. O consumo de memória ainda depende da quantidade de eventos nessas janelas.

Após o descarte de uma janela, o mesmo evento pode ser processado novamente. Nesse caso, sua janela é recriada e pode ser removida imediatamente quando o limite de retenção for reaplicado. O resultado continua sendo exibido em log.
