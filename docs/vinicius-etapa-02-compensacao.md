# Etapa 02 — Compensação da Reserva de Limite

## Objetivo

Implementar a compensação da reserva de limite quando uma proposta de crédito for recusada ou expirada.

A compensação devolve o valor reservado ao limite disponível do cliente e publica o evento `ReservaDeLimiteCancelada`.

> Importante: a compensação não é um rollback. A reserva original continua registrada como fato histórico. O sistema executa uma nova ação de negócio para desfazer o efeito da reserva.

---

## Fluxo implementado

```text
Limite inicial: R$ 10.000
        ↓
Elegibilidade aprovada: R$ 3.000
        ↓
Limite disponível: R$ 7.000
        ↓
Proposta recusada ou expirada
        ↓
Compensação da reserva
        ↓
Limite disponível: R$ 10.000
        ↓
Publicação de ReservaDeLimiteCancelada
```

---

## Eventos consumidos

### Proposta recusada

Tópico:

```text
credito.proposta.recusada.v1
```

Exemplo:

```json
{
  "eventoId": "evt-recusa-vinicius-02",
  "solicitacaoId": "sol-vinicius-02",
  "motivo": "RISCO_NAO_ACEITO",
  "dataRecusa": "2026-09-13T15:25:00-03:00"
}
```

Header:

```json
{
  "ce_id": "evt-recusa-vinicius-02"
}
```

### Proposta expirada

Tópico:

```text
credito.proposta.expirada.v1
```

---

## Evento publicado

Tópico:

```text
credito.reserva-limite.cancelada.v1
```

Evento:

```text
ReservaDeLimiteCancelada
```

Exemplo de conteúdo:

```json
{
  "eventoId": "...",
  "eventoOrigemId": "evt-recusa-vinicius-02",
  "solicitacaoId": "sol-vinicius-02",
  "clienteId": "cli-vinicius-02",
  "valorDevolvido": 3000.00,
  "limiteDisponivel": 10000.00,
  "motivo": "PROPOSTA_RECUSADA",
  "dataCancelamento": "..."
}
```

---

## Como executar o teste funcional

### 1. Subir a infraestrutura

Na raiz do projeto:

```powershell
docker compose up -d
```

Verificar:

```powershell
docker compose ps
```

### 2. Iniciar o serviço de crédito

```powershell
mvn -f servico-credito\pom.xml spring-boot:run
```

Kafka UI:

```text
http://localhost:8081
```

### 3. Criar limite inicial

```powershell
docker exec -i aed-equipe-05-postgres psql -U aed -d aed -c "INSERT INTO limite_credito (cliente_id, limite_total, limite_disponivel, atualizado_em) VALUES ('cli-vinicius-02', 10000.00, 10000.00, CURRENT_TIMESTAMP) ON CONFLICT (cliente_id) DO UPDATE SET limite_total = 10000.00, limite_disponivel = 10000.00, atualizado_em = CURRENT_TIMESTAMP;"
```

Conferir:

```powershell
docker exec -i aed-equipe-05-postgres psql -U aed -d aed -c "SELECT cliente_id, limite_total, limite_disponivel FROM limite_credito WHERE cliente_id = 'cli-vinicius-02';"
```

Resultado esperado:

```text
10000.00 disponíveis
```

### 4. Publicar ElegibilidadeAprovada

Tópico:

```text
credito.elegibilidade.aprovada.v1
```

Key:

```text
sol-vinicius-02
```

Value:

```json
{
  "eventoId": "evt-eleg-vinicius-02",
  "solicitacaoId": "sol-vinicius-02",
  "clienteId": "cli-vinicius-02",
  "valorAprovado": 3000.00,
  "dataAprovacao": "2026-09-13T15:20:00-03:00"
}
```

Header:

```json
{
  "ce_id": "evt-eleg-vinicius-02"
}
```

Conferir o limite:

```powershell
docker exec -i aed-equipe-05-postgres psql -U aed -d aed -c "SELECT cliente_id, limite_total, limite_disponivel FROM limite_credito WHERE cliente_id = 'cli-vinicius-02';"
```

Resultado esperado:

```text
7000.00 disponíveis
```

### 5. Publicar PropostaDeCreditoRecusada

Tópico:

```text
credito.proposta.recusada.v1
```

Key:

```text
sol-vinicius-02
```

Value:

```json
{
  "eventoId": "evt-recusa-vinicius-02",
  "solicitacaoId": "sol-vinicius-02",
  "motivo": "RISCO_NAO_ACEITO",
  "dataRecusa": "2026-09-13T15:25:00-03:00"
}
```

Header:

```json
{
  "ce_id": "evt-recusa-vinicius-02"
}
```

### 6. Conferir se o limite voltou

```powershell
docker exec -i aed-equipe-05-postgres psql -U aed -d aed -c "SELECT cliente_id, limite_total, limite_disponivel FROM limite_credito WHERE cliente_id = 'cli-vinicius-02';"
```

Resultado esperado:

```text
10000.00 disponíveis
```

### 7. Conferir que a reserva original continua existindo

```powershell
docker exec -i aed-equipe-05-postgres psql -U aed -d aed -c "SELECT solicitacao_id, cliente_id, valor_reservado, status FROM reserva_limite WHERE solicitacao_id = 'sol-vinicius-02';"
```

A reserva não deve ser apagada.

### 8. Conferir o registro de compensação

```powershell
docker exec -i aed-equipe-05-postgres psql -U aed -d aed -c "SELECT evento_origem_id, solicitacao_id, cliente_id, valor_devolvido, limite_disponivel_apos, motivo FROM cancelamento_reserva WHERE solicitacao_id = 'sol-vinicius-02';"
```

Resultado esperado:

```text
valor_devolvido = 3000.00
limite_disponivel_apos = 10000.00
motivo = PROPOSTA_RECUSADA
```

### 9. Conferir o evento no Kafka

Abrir:

```text
http://localhost:8081
```

Tópico:

```text
credito.reserva-limite.cancelada.v1
```

Verificar a mensagem `ReservaDeLimiteCancelada`.

---

## Por que isso não é rollback?

Rollback tenta fazer a operação anterior desaparecer como se nunca tivesse acontecido.

Neste fluxo, a reserva realmente aconteceu e continua registrada. Quando a proposta não pode prosseguir, o sistema executa uma nova ação de negócio que compensa o efeito anterior.

```text
LimiteDeCreditoReservado
        ↓
Proposta recusada/expirada
        ↓
ReservaDeLimiteCancelada
```

Esse padrão é apropriado para uma Saga distribuída porque os serviços trabalham de forma assíncrona e não compartilham uma única transação de banco.

---

## Validação

Etapa 02 validada com sucesso quando:

- o projeto compila;
- os testes automatizados passam;
- o serviço consome `PropostaDeCreditoRecusada` ou `PropostaDeCreditoExpirada`;
- o limite volta de R$ 7.000 para R$ 10.000;
- a reserva original permanece registrada;
- o cancelamento fica registrado separadamente;
- `ReservaDeLimiteCancelada` é publicado no Kafka.

---

## Encerrando o ambiente

Parar o Spring Boot:

```text
Ctrl + C
```

Opcionalmente encerrar os containers:

```powershell
docker compose down
```
