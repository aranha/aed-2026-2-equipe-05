# Contratos de eventos

Este documento traz o contrato do evento que abre o fluxo, `credito.solicitacao.solicitada.v1`, e o do evento de compensação da saga, `credito.reserva-limite.cancelada.v1`. Os demais eventos da saga estão em [contratos-da-saga.md](contratos-da-saga.md), com as mesmas regras.

# Evento de solicitação de crédito

## Tipo do evento

O tipo completo publicado no cabeçalho CloudEvents `ce_type` é:

```text
credito.solicitacao.solicitada.v1
```

Esse evento informa que uma solicitação de crédito foi recebida e publicada para processamento. Ele não significa que o crédito foi analisado, aprovado ou liberado.

## Campos da carga

| Campo | Tipo | Obrigatório | Significado |
|---|---|---:|---|
| `eventoId` | string (UUID) | Sim | Identifica unicamente esta ocorrência do evento e permite reconhecer reentregas da mesma publicação. É gerado pelo produtor. |
| `solicitacaoId` | string (UUID) | Sim | Identifica unicamente a solicitação de crédito que será acompanhada durante o fluxo. É gerado pelo produtor e também é usado como chave de partição. |
| `clienteId` | string | Sim | Identifica o cliente no domínio de crédito por meio de um identificador interno; não deve conter nome, CPF ou outro dado pessoal diretamente identificável. |
| `valorSolicitado` | decimal positivo | Sim | Representa o valor monetário do crédito solicitado pelo cliente, antes de análise, aprovação, juros, tarifas ou demais condições da proposta. |
| `dataSolicitacao` | string no formato ISO-8601 com offset | Sim | Indica o instante de ocorrência em que a solicitação foi registrada pelo produtor e é o relógio de domínio usado nas agregações por janela. |
| `canalOrigem` | string | Sim | Informa o canal pelo qual a solicitação foi recebida, por exemplo `APP`; novos valores podem ser acrescentados sem alterar o significado do campo. |

## Datas e horários

Todas as datas e horários são representados como texto no formato ISO-8601 com offset explícito, nunca como epoch. O produtor normaliza `dataSolicitacao` para o offset de Brasília (`-03:00`).

Exemplo:

```text
2026-08-23T14:30:00-03:00
```

## Chave de partição e ordenação

A chave usada na publicação no Kafka é `solicitacaoId`.

Essa escolha garante que todos os eventos que utilizem o mesmo `solicitacaoId` sejam enviados para a mesma partição e mantenham entre si a ordem de publicação observada pelo Kafka. Não existe garantia de ordem global entre partições nem de ordem entre solicitações diferentes, mesmo quando pertencem ao mesmo cliente.

## Compatibilidade

A regra escolhida é **FULL**: um consumidor novo deve continuar lendo eventos produzidos pela versão anterior e um consumidor antigo deve continuar lendo eventos produzidos pela versão nova.

Essa regra permite implantar produtores e os dois grupos de consumidores em qualquer ordem, sem exigir uma janela coordenada. Para preservá-la, novos campos devem ser opcionais ou possuir valor padrão, consumidores devem ignorar campos desconhecidos e campos obrigatórios existentes não devem ser removidos, renomeados nem ter tipo ou significado alterado dentro da versão `v1`.

Por exemplo, alterar `valorSolicitado` para incluir juros ou tarifas seria uma mudança incompatível de significado, mesmo que o campo continuasse sendo decimal. Nesse caso, seria necessário publicar uma nova versão do tipo do evento e manter a transição compatível entre produtores e consumidores.

## Contexto organizacional da regra de compatibilidade

A regra FULL não é uma escolha isolada de formato de dado — ela existe porque o `servico-credito` e o `servico-risco` são times/deploys diferentes dentro do mesmo repositório, cada um com seu próprio pipeline de build e ciclo de release.

- **Quantos consumidores dependem deste evento hoje:** dois grupos de consumo distintos no `servico-risco` — o consumidor de análise de crédito (grupo padrão) e o consumidor de fluxo por janela de tempo (`risco-fluxo-creditos-v1`). Ambos leem o mesmo tópico `credito.solicitacao.solicitada.v1` a partir do mesmo produtor.
- **Quem controla o deploy de cada lado:** o `servico-credito` publica o evento e pode ser implantado de forma independente do `servico-risco`; não existe um gate de deploy compartilhado nem uma janela coordenada entre os dois serviços. Como cada serviço tem seu próprio `pom.xml`, artefato `.jar` e ciclo de subida (ver `README.md`), qualquer um dos dois pode subir uma versão nova sem avisar o outro.
- **Por que isso exige FULL, e não FORWARD ou BACKWARD isoladamente:** sem um mecanismo de coordenação de deploy, a ordem de subida entre produtor e os dois grupos de consumidores é imprevisível — um consumidor pode ser reiniciado antes do produtor, ou depois, e um novo consumidor pode subir enquanto um consumidor antigo do outro grupo ainda está rodando. FULL é a única regra que garante leitura correta em qualquer uma dessas combinações, porque não assume qual lado sobe primeiro.

## Exemplo de carga

Todos os valores abaixo são fictícios:

```json
{
  "eventoId": "6fd49859-f932-4f3e-9941-4609b46a65aa",
  "solicitacaoId": "75ae6c98-2856-4717-9842-33a6f1f70953",
  "clienteId": "cli-ficticio-001",
  "valorSolicitado": 15000.00,
  "dataSolicitacao": "2026-08-23T14:30:00-03:00",
  "canalOrigem": "APP"
}
```

# Evento de compensação: ReservaDeLimiteCancelada

## Tipo do evento

```text
credito.reserva-limite.cancelada.v1
```

Publicado pelo `servico-credito` (`ce_source=/credito/limites`) quando uma proposta é recusada ou expira depois de o limite ter sido reservado. Informa que a reserva foi desfeita e o valor voltou ao limite disponível do cliente. É um fato novo: a reserva original não é apagada nem reescrita, e o `LimiteDeCreditoReservado` que a criou continua no tópico dele. Consumido pelo `servico-risco` (grupo `risco-saga-reserva-v1`), que marca a análise como `CANCELADA`.

## Campos da carga

| Campo | Tipo | Obrigatório | Significado |
|---|---|---:|---|
| `eventoId` | string (UUID) | Sim | Identidade do cancelamento, igual ao `ce_id`. Uma reentrega do mesmo gatilho republica este mesmo `eventoId`, e os consumidores deduplicam por ele. |
| `eventoOrigemId` | string (UUID) | Sim | `eventoId` da recusa ou da expiração que provocou o cancelamento. É a chave que impede devolver o limite duas vezes pelo mesmo gatilho. |
| `solicitacaoId` | string (UUID) | Sim | Solicitação cuja reserva foi cancelada; é também a chave de partição. |
| `clienteId` | string | Sim | Identificador interno do cliente que recebeu o limite de volta; não contém dado pessoal. |
| `valorDevolvido` | decimal positivo | Sim | Quanto voltou ao limite disponível; é igual ao valor da reserva cancelada. |
| `limiteDisponivel` | decimal | Sim | Limite disponível do cliente **depois** da devolução. |
| `motivo` | string | Sim | `PROPOSTA_RECUSADA` ou `PROPOSTA_EXPIRADA`, conforme o gatilho. Novos valores podem ser acrescentados sem mudar o significado do campo. |
| `dataCancelamento` | string no formato ISO-8601 com offset | Sim | Instante em que a compensação foi gravada, no offset de Brasília (`-03:00`); é também o `ce_time`. |

## Chave de partição e ordenação

A chave é `solicitacaoId`, a mesma do `CreditoSolicitado`. Os eventos de uma solicitação ficam na mesma partição do tópico de compensação, mas não há ordem entre tópicos: um consumidor pode receber o cancelamento antes da reserva, e as transições de status no `servico-risco` só avançam para que isso não desfaça um cancelamento.

## Compatibilidade

**FULL**, pelas mesmas razões do evento de solicitação: produtor e consumidor sobem em qualquer ordem. Campo novo entra como opcional; nenhum campo existente é removido, renomeado ou muda de tipo ou de significado dentro da `v1`. Se `limiteDisponivel` passasse a representar o saldo antes da devolução, o esquema continuaria válido e todo consumidor mostraria o número errado: essa mudança exige `credito.reserva-limite.cancelada.v2`.

## Exemplo de carga

Todos os valores abaixo são fictícios:

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
