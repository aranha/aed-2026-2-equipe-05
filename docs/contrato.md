# Contrato do evento de solicitação de crédito

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
