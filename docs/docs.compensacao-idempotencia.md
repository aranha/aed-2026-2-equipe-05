# Etapa 03 — Idempotência e Reexecução da Compensação

## Objetivo

Garantir que a compensação da reserva de limite seja idempotente e reexecutável pelo `eventoId`.

Isso significa que, se o mesmo evento Kafka for entregue mais de uma vez, o limite do cliente não pode ser devolvido novamente.

Também significa que, caso a compensação tenha sido persistida no banco mas a publicação do evento de saída falhe, uma nova tentativa deve reutilizar a compensação já registrada e tentar publicar novamente, sem duplicar o crédito.

---

## Comportamento esperado

```text
Limite antes da compensação: R$ 7.000

Evento de recusa:
eventoId = evt-recusa-vinicius-03

1ª execução
↓
devolve R$ 3.000
↓
limite = R$ 10.000
↓
registra a compensação

Mesmo evento entregue novamente
↓
sistema encontra eventoId já processado
↓
não devolve R$ 3.000 novamente
↓
limite continua = R$ 10.000
```

O comportamento incorreto seria:

```text
R$ 7.000
↓
1ª compensação
R$ 10.000
↓
mesmo evento novamente
R$ 13.000 ❌
```

---

## Estratégia de idempotência

A chave de deduplicação é:

```text
eventoId
```

O evento de origem da compensação é persistido como `evento_origem_id`.

Antes de executar uma nova compensação, o serviço consulta se já existe um cancelamento para o mesmo evento.

Fluxo conceitual:

```text
recebe evento
↓
consulta evento_origem_id
↓
já existe?
├─ sim → reutiliza compensação existente
└─ não → executa compensação e persiste
```

---

## Arquivos alterados nesta etapa

```text
servico-credito/src/main/java/br/pucminas/aed/credito/service/
├── CancelamentoReservaRepository.java
├── CompensacaoReservaService.java
└── ReservaCanceladaPublicacaoService.java

servico-credito/src/test/java/br/pucminas/aed/credito/
└── CompensacaoReservaIntegracaoTest.java
```

---

## Testes automatizados

Executar:

```powershell
mvn -f servico-credito\pom.xml clean test
```

Resultado esperado:

```text
BUILD SUCCESS
```

Os testes cobrem:

```text
- reexecução do mesmo evento sem devolver o limite duas vezes
- nova tentativa de publicação quando a compensação já foi persistida
```

---

## Teste funcional

### 1. Subir a infraestrutura

```powershell
docker compose up -d
```

### 2. Iniciar o serviço

```powershell
mvn -f servico-credito\pom.xml spring-boot:run
```

Kafka UI:

```text
http://localhost:8081
```

---

## Cenário de teste

Cliente:

```text
cli-vinicius-03
```

Solicitação:

```text
sol-vinicius-03
```

Limite inicial:

```text
R$ 10.000
```

Valor reservado:

```text
R$ 3.000
```

---

## 3. Criar limite inicial

```powershell
docker exec -i aed-equipe-05-postgres psql -U aed -d aed -c "INSERT INTO limite_credito (cliente_id, limite_total, limite_disponivel, atualizado_em) VALUES ('cli-vinicius-03', 10000.00, 10000.00, CURRENT_TIMESTAMP) ON CONFLICT (cliente_id) DO UPDATE SET limite_total = 10000.00, limite_disponivel = 10000.00, atualizado_em = CURRENT_TIMESTAMP;"
```

Conferir:

```powershell
docker exec -i aed-equipe-05-postgres psql -U aed -d aed -c "SELECT cliente_id, limite_total, limite_disponivel FROM limite_credito WHERE cliente_id = 'cli-vinicius-03';"
```

Esperado:

```text
10000.00
```

---

## 4. Publicar ElegibilidadeAprovada

Tópico:

```text
credito.elegibilidade.aprovada.v1
```

Key:

```text
sol-vinicius-03
```

Value:

```json
{
  "eventoId": "evt-eleg-vinicius-03",
  "solicitacaoId": "sol-vinicius-03",
  "clienteId": "cli-vinicius-03",
  "valorAprovado": 3000.00,
  "dataAprovacao": "2026-09-13T15:40:00-03:00"
}
```

Header:

```json
{
  "ce_id": "evt-eleg-vinicius-03"
}
```

Depois da reserva:

```text
limite disponível = R$ 7.000
```

---

## 5. Publicar PropostaDeCreditoRecusada

Tópico:

```text
credito.proposta.recusada.v1
```

Key:

```text
sol-vinicius-03
```

Value:

```json
{
  "eventoId": "evt-recusa-vinicius-03",
  "solicitacaoId": "sol-vinicius-03",
  "motivo": "RISCO_NAO_ACEITO",
  "dataRecusa": "2026-09-13T15:45:00-03:00"
}
```

Header:

```json
{
  "ce_id": "evt-recusa-vinicius-03"
}
```

Depois da primeira compensação:

```text
limite disponível = R$ 10.000
```

---

## 6. Reenviar exatamente o mesmo evento

Publicar novamente a mesma mensagem, mantendo:

```text
eventoId = evt-recusa-vinicius-03
```

e:

```json
{
  "ce_id": "evt-recusa-vinicius-03"
}
```

Conferir novamente:

```powershell
docker exec -i aed-equipe-05-postgres psql -U aed -d aed -c "SELECT cliente_id, limite_total, limite_disponivel FROM limite_credito WHERE cliente_id = 'cli-vinicius-03';"
```

Resultado esperado:

```text
10000.00
```

O limite não pode chegar a R$ 13.000.

---

## 7. Conferir deduplicação

```powershell
docker exec -i aed-equipe-05-postgres psql -U aed -d aed -c "SELECT evento_origem_id, COUNT(*) FROM cancelamento_reserva WHERE evento_origem_id = 'evt-recusa-vinicius-03' GROUP BY evento_origem_id;"
```

Resultado esperado:

```text
evt-recusa-vinicius-03 | 1
```

Isso comprova que o mesmo `eventoId` gerou apenas uma compensação persistida.

---

## Resultado validado

```text
Limite inicial           R$ 10.000
Após reserva             R$  7.000
Após 1ª compensação      R$ 10.000
Após evento duplicado    R$ 10.000
Cancelamentos gravados            1
```

---

## Por que isso é importante?

Em sistemas distribuídos, Kafka pode reentregar mensagens.

Sem idempotência, uma reentrega de `PropostaDeCreditoRecusada` poderia devolver o mesmo valor mais de uma vez e criar um saldo incorreto.

A deduplicação por `eventoId` garante que a compensação produza o mesmo resultado mesmo quando o evento é processado repetidamente.

---

## Encerrando o ambiente

Parar o Spring Boot:

```text
Ctrl + C
```

Opcionalmente:

```powershell
docker compose down
```
