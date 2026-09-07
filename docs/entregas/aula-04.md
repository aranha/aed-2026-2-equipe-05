# Aula 04 - Chave de partição e janela de agregação

## O que foi feito

Nesta etapa a equipe registrou em ADR a justificativa da chave de partição do evento `credito.solicitacao.solicitada.v1` e documentou a janela escolhida para a agregação de fluxo construída na aula 03. Não houve alteração de código: o agregador em grupo próprio (`FluxoCreditoSolicitadoListener` e `FluxoCreditoSolicitadoService`) continua o mesmo, e esta entrega explica as decisões que ele já carrega.

## Por onde começar a leitura

1. [ADR-003 - Chave de partição](../adr/ADR-003-chave-de-particao.md): qual pergunta do negócio a chave `solicitacaoId` responde sem repartir, qual deixou de responder e o que a equipe faria se essa segunda pergunta virasse prioridade.
2. [CreditoService](../../servico-credito/src/main/java/br/pucminas/aed/credito/service/CreditoService.java): onde a chave é enviada no `ProducerRecord`.
3. [FluxoCreditoSolicitadoService](../../servico-risco/src/main/java/br/pucminas/aed/risco/service/FluxoCreditoSolicitadoService.java): a janela fixa de cinco minutos e o alinhamento ao relógio.
4. [Folha de rosto da aula 03](aula-03.md): a escolha do relógio de ocorrência e o comportamento do retardatário, que continuam valendo.
5. [Registro de uso de IA](../IA.md), seção Aula 04.

## O que foi agregado, com qual janela

A agregação responde: **qual foi o volume de crédito solicitado, em quantidade e em valor, a cada janela fixa de cinco minutos?**

- **Janela:** fixa (tumbling) de cinco minutos, alinhada ao relógio em `:00`, `:05`, `:10`, e não ao instante em que o serviço subiu. Duas instâncias que agregassem o mesmo fluxo produziriam os mesmos limites de janela.
- **Relógio:** ocorrência, pelo campo `dataSolicitacao` do evento. Reprocessar o tópico do começo produz as mesmas janelas com os mesmos totais.
- **Resultado observável:** log do `servico-risco`, uma linha por evento com a janela, a quantidade acumulada e o total acumulado.

## Por que essa janela

- **Fixa, e não por salto (hopping):** as janelas fixas não se sobrepõem, então a soma de todas elas é o total solicitado no período. Numa janela por salto cada evento cai em várias janelas e a soma dos relatórios passa a valer múltiplas vezes o faturamento real. Para uma pergunta de volume por intervalo, a soma precisa fechar.
- **Fixa, e não deslizante (sliding):** a janela deslizante nasce em cada evento e tem limites não redondos. Ela responde "quanto foi solicitado nos cinco minutos anteriores a este pedido", que não é a pergunta da área de negócio.
- **Fixa, e não de sessão:** sessão é sempre de alguém. Agrupar por sessão exigiria agregar por `clienteId`, e o ADR-003 mostra que isso não fecha com a chave atual sem repartir o fluxo.
- **Cinco minutos:** granularidade suficiente para acompanhar picos de solicitação num painel operacional sem gerar uma janela por evento. É parâmetro de negócio e pode mudar sem alterar o desenho.

## O que a chave permite e o que não permite

A chave `solicitacaoId` garante a ordem dos eventos de uma mesma solicitação e distribui as solicitações pelas três partições. A agregação atual é da carteira inteira, por isso está correta com uma instância lendo as três partições. Agregar por cliente exigiria republicar o fluxo chaveado por `clienteId`, e a equipe decidiu não pagar esse custo enquanto não houver pergunta de negócio que o justifique.

## Como rodar

Os comandos estão no [README](../../README.md), seções "Como rodar o projeto" e "Consumidor de fluxo por janela de tempo". Para ver a chave e a partição de cada evento, abra a Kafka UI em `http://localhost:8081`, tópico `credito.solicitacao.solicitada.v1`, aba Messages: a coluna Key mostra o `solicitacaoId` e a coluna Partition mostra onde ele caiu.

## Quem fez o quê

| Integrante | Contribuição nesta etapa |
|---|---|
| Paulo Euclydes Aranha Junior | Liderança da equipe e revisão antes da entrega. |
| Vinícius Eduardo Silva Oliveira | Participação na revisão da entrega. |
| Marcus Vinicius da Cruz Santos | Participação na revisão da entrega. |
| Rafael Oliveira de Lima | Participação na revisão da entrega. |
| Guilherme Nunes Faria | Participação na revisão da entrega. |
| Hugo Fontolan Piani | Participação na revisão da entrega. |
| Sesaque de Oliveira da Cruz | Redação da ADR-003. |
