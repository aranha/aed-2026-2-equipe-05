# Aula 05 - Log append-only e a decisão sobre Event Sourcing

## O que foi feito

Esta etapa não acrescentou código: ela registra, em ADR, uma decisão que o sistema já
carregava implicitamente desde a aula 02 — **o estado do domínio é guardado em tabelas, e
não reconstruído por replay de um event store**.

A decisão em si foi tomada durante a Parte B do projeto final, quando surgiu a pergunta
"em que passo está esta solicitação". A [ADR-005](../adr/ADR-005-event-sourcing.md) reúne
a decisão, as alternativas descartadas e o que se aceitou perder com ela.

## Por onde começar a leitura

1. [ADR-005 - Event Sourcing](../adr/ADR-005-event-sourcing.md): o que guardamos como fato
   e o que guardamos como estado, e o que isso custa.
2. [ADR-002 - Domínio](../adr/ADR-002-dominio.md), seção da compensação: onde o princípio
   append-only entra no domínio — desfazer é publicar um fato novo, nunca apagar o anterior.
3. [`CompensacaoReservaService`](../../servico-credito/src/main/java/br/pucminas/aed/credito/service/CompensacaoReservaService.java):
   o princípio no código. A compensação grava uma linha nova em `cancelamento_reserva` e
   devolve o limite; a linha original de `reserva_limite` continua lá.
4. [`docs/IA.md`](../IA.md), seção "Etapa final (Partes A, B e C)": a recusa registrada da
   sugestão de tornar a Solicitação de Crédito um agregado event sourced.
5. [`arquitetura.md`](../arquitetura.md), seção 8: a projeção persistida do agregador, que
   é a parte de CQRS que ficou adiada e não recusada.

## O princípio no sistema: o que é fato e o que é estado

**Fato** é o que foi publicado num tópico. Ele não muda. `LimiteDeCreditoReservado` e
`ReservaDeLimiteCancelada` são dois fatos independentes: o segundo não corrige nem substitui
o primeiro, ele registra que o efeito do primeiro foi desfeito. Quem leu só o primeiro não
leu uma mentira — leu o que era verdade naquele instante.

**Estado** é o que as tabelas guardam depois de processar os fatos. `reserva_limite.status`
sai de `RESERVADA` para `CANCELADA` porque é projeção do desfecho, não o fato em si.

A diferença aparece na consulta. Para saber *o que aconteceu*, o lugar é o tópico. Para
saber *como está agora*, o lugar é a tabela. Um sistema event sourced junta os dois num
lugar só, e é exatamente essa junção que decidimos não fazer.

## Por que não Event Sourcing

O resumo está na ADR-005; os três custos que pesaram:

- **Dois modelos de persistência** para manter e evoluir, quando um responde as perguntas
  que o negócio faz hoje.
- **Versionamento dos eventos gravados.** O `contrato.md` já prevê que o esquema muda; num
  event store, cada mudança exige *upcasting* dos eventos antigos na leitura.
- **Dado pessoal num log imutável.** O `clienteId` é um identificador interno justamente
  para evitar isso, mas um event store como fonte da verdade torna a correção e a remoção
  de qualquer dado um problema de design, não de operação.

O que se perde está nomeado na ADR: perguntas novas sobre o passado só são respondidas se
o dado tiver sido guardado na época.

## O que a retenção do Kafka significa aqui

Os tópicos têm a retenção padrão do broker, sete dias. Isso é suficiente para reprocessar
uma DLQ ou reconstruir a agregação por janela, que são os dois usos reais de releitura no
sistema. Não é suficiente para tratar o log como arquivo histórico — e a ADR-005 aceita
isso explicitamente, em vez de deixar a suposição implícita.

## Como rodar

Esta etapa é documental e não altera o comportamento do sistema. Para ver o princípio
funcionando, o roteiro da compensação está no [README](../../README.md) e em
[`docs/saga/compensacao-da-reserva.md`](../saga/compensacao-da-reserva.md): depois de
compensar, `reserva_limite` mostra a reserva com `status = 'CANCELADA'` e
`cancelamento_reserva` mostra o fato novo, lado a lado.

## Quem fez o quê

| Integrante | Contribuição nesta etapa |
|---|---|
| Paulo Euclydes Aranha Junior | Liderança da equipe e revisão antes da entrega. |
| Vinícius Eduardo Silva Oliveira | Participação na revisão da entrega. |
| Marcus Vinicius da Cruz Santos | Participação na revisão da entrega. |
| Rafael Oliveira de Lima | Participação na revisão da entrega. |
| Guilherme Nunes Faria | Participação na revisão da entrega. |
| Hugo Fontolan Piani | Participação na revisão da entrega. |
| Sesaque de Oliveira da Cruz | Participação na revisão da entrega. |
