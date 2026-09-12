# Etapa 01 — Reserva de Limite de Crédito

## Objetivo

Esta etapa implementa a reserva de limite de crédito quando uma proposta recebe elegibilidade aprovada.

O fluxo esperado é:

```text
ElegibilidadeAprovada
        ↓
servico-credito consome o evento
        ↓
consulta o limite disponível do cliente
        ↓
reserva o valor aprovado
        ↓
atualiza o limite disponível
        ↓
registra a reserva
        ↓
publica LimiteDeCreditoReservado
```

Exemplo utilizado nos testes:

```text
Limite inicial:      R$ 10.000,00
Valor aprovado:      R$  3.000,00
Limite após reserva: R$  7.000,00
```

---

## Pré-requisitos

- Java 21
- Maven 3.9.x
- Docker Desktop em execução
- Docker Compose
- Projeto clonado localmente

Validar o ambiente:

```powershell
java -version
mvn -version
docker --version
docker compose version
```

O Java e o Maven devem utilizar o JDK 21.

---

## Branch utilizada

A implementação foi desenvolvida em uma branch própria:

```text
feature/vinicius-reserva-limite
```

Para confirmar a branch atual:

```powershell
git branch
```

---

## Arquivos da Etapa 01

### Arquivos existentes modificados

```text
servico-credito/pom.xml
servico-credito/src/main/java/br/pucminas/aed/credito/CreditoConfig.java
servico-credito/src/main/java/br/pucminas/aed/credito/service/CreditoService.java
servico-credito/src/main/java/br/pucminas/aed/credito/service/ResultadoPublicacaoService.java
servico-credito/src/main/resources/application.yml
```

### Arquivos novos

```text
servico-credito/src/main/java/br/pucminas/aed/credito/controller/ElegibilidadeAprovadaListener.java
servico-credito/src/main/java/br/pucminas/aed/credito/domain/ElegibilidadeAprovadaEvent.java
servico-credito/src/main/java/br/pucminas/aed/credito/domain/LimiteDeCreditoReservadoEvent.java
servico-credito/src/main/java/br/pucminas/aed/credito/service/LimiteCreditoRepository.java
servico-credito/src/main/java/br/pucminas/aed/credito/service/LimiteReservadoPublicacaoService.java
servico-credito/src/main/java/br/pucminas/aed/credito/service/ReservaLimiteRepository.java
servico-credito/src/main/java/br/pucminas/aed/credito/service/ReservaLimiteService.java
servico-credito/src/main/resources/schema.sql
servico-credito/src/test/java/br/pucminas/aed/credito/ReservaLimiteIntegracaoTest.java
```

---

## 1. Subir a infraestrutura

Na raiz do projeto:

```powershell
docker compose up -d
```

Conferir os containers:

```powershell
docker compose ps
```

Os serviços de infraestrutura devem estar com status de execução normal.

---

## 2. Executar os testes automatizados

Na raiz do projeto:

```powershell
mvn -f servico-credito\pom.xml clean test
```

Resultado esperado:

```text
BUILD SUCCESS
```

O teste da Etapa 01 valida o cenário de reserva de limite.

---

## 3. Subir o servico-credito

Em um terminal separado:

```powershell
mvn -f servico-credito\pom.xml spring-boot:run
```

Manter esse terminal aberto durante o teste funcional.

---

## 4. Cadastrar um limite inicial para o cliente

Em outro terminal, executar:

```powershell
docker exec -i aed-equipe-05-postgres psql -U aed -d aed -c "INSERT INTO limite_credito (cliente_id, limite_total, limite_disponivel, atualizado_em) VALUES ('cli-vinicius-01', 10000.00, 10000.00, CURRENT_TIMESTAMP) ON CONFLICT (cliente_id) DO UPDATE SET limite_total = 10000.00, limite_disponivel = 10000.00, atualizado_em = CURRENT_TIMESTAMP;"
```

Conferir o limite antes da reserva:

```powershell
docker exec -i aed-equipe-05-postgres psql -U aed -d aed -c "SELECT cliente_id, limite_total, limite_disponivel FROM limite_credito WHERE cliente_id = 'cli-vinicius-01';"
```

Resultado esperado:

```text
cliente_id       | limite_total | limite_disponivel
-----------------+--------------+------------------
cli-vinicius-01  |     10000.00 |         10000.00
```

---

## 5. Publicar o evento ElegibilidadeAprovada

Abrir o Kafka UI:

```text
http://localhost:8081
```

Acessar:

```text
Topics
→ credito.elegibilidade.aprovada.v1
→ Produce Message
```

### Key

```text
sol-vinicius-01
```

### Value

```json
{
  "eventoId": "evt-eleg-vinicius-01",
  "solicitacaoId": "sol-vinicius-01",
  "clienteId": "cli-vinicius-01",
  "valorAprovado": 3000.00,
  "dataAprovacao": "2026-09-12T15:10:00-03:00"
}
```

### Headers

Nesta versão do Kafka UI, os headers devem ser informados em formato JSON:

```json
{
  "ce_id": "evt-eleg-vinicius-01"
}
```

Depois clicar em **Produce Message**.

---

## 6. Conferir o limite depois da reserva

Executar:

```powershell
docker exec -i aed-equipe-05-postgres psql -U aed -d aed -c "SELECT cliente_id, limite_total, limite_disponivel FROM limite_credito WHERE cliente_id = 'cli-vinicius-01';"
```

Resultado esperado:

```text
cliente_id       | limite_total | limite_disponivel
-----------------+--------------+------------------
cli-vinicius-01  |     10000.00 |          7000.00
```

---

## 7. Conferir a reserva registrada

Executar:

```powershell
docker exec -i aed-equipe-05-postgres psql -U aed -d aed -c "SELECT solicitacao_id, evento_origem_id, cliente_id, valor_reservado, limite_disponivel_apos, status FROM reserva_limite WHERE solicitacao_id = 'sol-vinicius-01';"
```

Resultado esperado:

```text
solicitacao_id         = sol-vinicius-01
evento_origem_id       = evt-eleg-vinicius-01
cliente_id             = cli-vinicius-01
valor_reservado        = 3000.00
limite_disponivel_apos = 7000.00
status                  = RESERVADA
```

---

## 8. Conferir o evento LimiteDeCreditoReservado

No Kafka UI, acessar:

```text
Topics
→ credito.limite.reservado.v1
→ Messages
```

Deve existir uma mensagem correspondente à reserva, contendo dados equivalentes a:

```json
{
  "solicitacaoId": "sol-vinicius-01",
  "clienteId": "cli-vinicius-01",
  "valorReservado": 3000.00,
  "limiteDisponivel": 7000.00
}
```

O `eventoId` do evento de saída é gerado pelo próprio serviço.

---

## 9. Resultado esperado da Etapa 01

Ao final do teste funcional, deve ser possível comprovar:

```text
Limite antes da reserva:  R$ 10.000,00
Valor reservado:          R$  3.000,00
Limite após a reserva:    R$  7.000,00
Status da reserva:        RESERVADA
Evento publicado:         LimiteDeCreditoReservado
```

---

## 10. Encerrar os serviços

Parar o Spring Boot no terminal em que ele está rodando:

```text
Ctrl + C
```

Depois, se não for continuar desenvolvendo imediatamente, parar a infraestrutura Docker:

```powershell
docker compose down
```

Se quiser manter Kafka e PostgreSQL para continuar trabalhando, não é obrigatório executar `docker compose down`.

---

## 11. Salvar a implementação no Git

Depois que os testes automatizados e funcionais estiverem concluídos:

```powershell
git add servico-credito
```

Conferir:

```powershell
git status
```

Criar o commit:

```powershell
git commit -m "feat: implementa reserva de limite de credito"
```

Enviar a branch:

```powershell
git push -u origin feature/vinicius-reserva-limite
```

---

## Observação arquitetural

A Etapa 01 implementa apenas a reserva de limite após a aprovação da elegibilidade. A compensação da Saga — devolução do limite em caso de proposta recusada ou expirada — será implementada em etapa posterior.

A compensação não deve apagar o fato original nem executar `DELETE` como forma de desfazer a reserva. O sistema deverá registrar um novo fato de negócio representando o cancelamento da reserva.
