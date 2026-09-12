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

Se o mesmo conjunto de eventos fosse reprocessado do começo, uma vez, com o estado em memória vazio, os totais finais por janela seriam os mesmos, porque a agregação usa o horário de ocorrência (`dataSolicitacao`) e os valores do próprio evento.

O desenho atual, porém, não persiste o resultado da agregação e também não faz deduplicação específica nesse consumidor de fluxo. Portanto, se houver eventos duplicados no tópico ou se o mesmo fluxo for reprocessado por cima de um estado já acumulado, os valores podem ser contados novamente. Para esta etapa isso é aceitável, porque o objetivo escolhido foi observar o fluxo por log de forma simples, sem transformar a agregação em uma projeção persistida ou exatamente uma vez.
