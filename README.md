# Banking Microservices Platform

> A complete, production-shaped banking platform built as a set of Spring Boot microservices: a
> customer opens an account, deposits money, and transfers it to someone else — with one-time
> passwords, e-mail verification, a real double-entry ledger, and a saga that keeps the money
> consistent across five services.

The platform is **10 Spring Boot services**, a **static web front end**, and a **Kafka event bus**,
orchestrated with **Eureka** service discovery and fronted by a **reactive API gateway**.

---

## Table of Contents

- [What It Does](#what-it-does)
- [Architecture at a Glance](#architecture-at-a-glance)
- [Service Map](#service-map)
- [Tech Stack](#tech-stack)
- [Prerequisites](#prerequisites)
- [Quick Start](#quick-start)
- [Running the Services](#running-the-services)
- [Configuration](#configuration)
- [API Overview](#api-overview)
- [Kafka Topics](#kafka-topics)
- [Databases](#databases)
- [The Web UI](#the-web-ui)
- [Observability](#observability)
- [Testing](#testing)
- [Repository Layout](#repository-layout)
- [Troubleshooting](#troubleshooting)
- [Security Notes](#security-notes)
- [Contributing](#contributing)
- [License](#license)

---

## What It Does

A customer can:

1. **Register** with an e-mail address and a password. The account is created `PENDING` and a
   verification link is e-mailed out.
2. **Verify** the e-mail address. The link contains a SHA-256 hashed, single-use token with a
   24-hour TTL.
3. **Log in** and receive a JWT bearer token valid for one hour.
4. **Open a savings or current account**, in USD, with a maximum of five accounts per user.
5. **Deposit** and **withdraw** money. Both are recorded in the ledger.
6. **Transfer** money to another account — optionally to another user's account — which triggers a
   fee calculation, a one-time password sent by e-mail, and a confirmation step.
7. **Watch a running transaction** move through its states in the web UI until it reaches a final
   outcome.

Behind that, the platform implements the things that make a banking system real:

- **A double-entry ledger.** `core-banking-service` is the *only* component allowed to change a
  balance. Every movement is a balanced pair of ledger entries against a transaction record with a
  unique `referenceId`, so the same transfer can never be applied twice.
- **A saga.** A transfer spans five services. `fund-transfer-service` drives it as a state machine
  with per-step timeouts, compensating reversals, and a sweeper that rescues transfers stuck
  mid-flight.
- **An event bus.** 22 Kafka topics (11 business + 11 dead-letter). Services communicate through
  commands and facts, with a transactional outbox so a database commit and a message publish
  cannot drift apart.
- **Real resilience.** Resilience4j circuit breakers and retries on downstream calls, idempotent
  consumers with a `processed_events` table, and Kafka producers configured for at-least-once
  delivery with `enable.idempotence`.
- **Full observability.** OpenTelemetry tracing, Prometheus metrics, structured JSON logs shipped to
  Loki, and a pre-provisioned Grafana stack with dashboards for traffic, logs, traces, and the
  event bus.

---

## Architecture at a Glance

![Animated architecture overview of the banking platform: a browser client calls the API gateway, which fans out to user, account, fund-transfer, OTP, notification, charge-fee and core-banking services; those services write to PostgreSQL and Redis and publish to Kafka, which feeds Saga + Outbox. An observability panel sits at the lower left.](docs/architecture.gif)

Every connector is lit and the dashes crawl steadily forward along each path, so the
diagram reads as continuous flow rather than a staged animation. The colour of a
connector says what kind of traffic it carries:

| Colour | Meaning |
| --- | --- |
| Cyan | Synchronous HTTP — the gateway calling a service |
| Violet | Asynchronous event — a Kafka publish or consume |
| Amber | Data store — a read or write to PostgreSQL or Redis |

```
                        ┌───────────────────────────┐
                        │      banking-web-ui       │
                        │   (static HTML + JS)      │
                        │      :3000                │
                        └─────────────┬─────────────┘
                                      │  Authorization: Bearer <jwt>
                                      ▼
      ┌───────────────────────────────────────────────────────────────┐
      │                    api-gateway  :8080                        │
      │   WebFlux · JWT validation · CORS · Redis rate limiting      │
      │   routes: /api/auth/** /api/accounts/** /api/transfers/**     │
      │   blocks: /internal/**  →  404                               │
      └───┬───────────────────┬────────────────────┬──────────────────┘
          │ lb://              │ lb://               │ lb://
          ▼                   ▼                    ▼
   ┌─────────────┐    ┌──────────────┐    ┌────────────────────┐
   │ user-service│    │account-service│   │ fund-transfer-svc  │
   │    :8083    │    │    :8081      │   │      :8082         │
   └──────┬──────┘    └───────┬──────┘   └─────────┬──────────┘
          │                   │                    │
          │              ┌────▼─────┐        ┌─────▼──────┐
          │              │  otp-   │        │ charge-fee │
          │              │ service │        │  service   │
          │              │  :8086  │        │   :8085    │
          │              └────┬─────┘        └────────────┘
          │                   │
          │              ┌────▼──────────────┐
          │              │ notification-svc  │
          │              │     :8087         │
          │              └───────────────────┘
          │
          │        ┌────────────────────────────┐
          └───────▶│  core-banking-service     │  ← the ledger
                   │          :8084             │    (only writer of balances)
                   └────────────────────────────┘

   Service discovery:  discovery-service :8761  (Eureka)
   Event bus:          Kafka :9092              (event-bus-service :8090 owns the topics)
   Persistence:        PostgreSQL :5433         (5 databases)
   Cache / OTP state:  Redis :6379
```

A transfer in flight looks like this — the saga is driven from the middle, and every step is a
command on the bus with its answer as a fact:

```
 fund-transfer-service ──▶ cbs.commands      ──▶ core-banking-service
                    ◀──   cbs.events        ──┘
                    ──▶ fee.commands       ──▶ charge-fee-service
                    ◀──   fee.events        ──┘
                    ──▶ otp.commands       ──▶ otp-service
                    ◀──   otp.events        ──┘   (encrypted OTP)
                                                   └──▶ notification-service ──▶ e-mail
                    ──▶ transfer.events     ──▶ notification-service, others
```

---

## Service Map

| Service | Port | Actuator | Database | Role |
|---|---|---|---|---|
| `discovery-service` | 8761 | 18761 | — | Eureka service registry. **Start first.** |
| `event-bus-service` | 8090 | 18090 | — | Owns the Kafka topic catalog (22 topics). **Start second.** |
| `api-gateway` | 8080 | 18080 | — | Single front door: routing, JWT validation, CORS, rate limiting |
| `user-service` | 8083 | 18083 | `user_db` | Registration, e-mail verification, login, JWT issuing |
| `account-service` | 8081 | 18081 | `account_db` | Customer accounts, account limits, cash account validation |
| `fund-transfer-service` | 8082 | 18082 | `fund_transfer_db` | Transfer orchestrator and saga engine |
| `core-banking-service` | 8084 | 18084 | `cbs_db` | Authoritative double-entry ledger — the only balance writer |
| `charge-fee-service` | 8085 | 18085 | — | Fee calculation policies (stateless) |
| `otp-service` | 8086 | 18086 | — (Redis) | One-time password lifecycle |
| `notification-service` | 8087 | 18087 | `notification_db` | Outbound e-mail delivery and delivery logs |
| `banking-web-ui` | 3000 | — | — | Static browser front end (no build step) |

Each service also has its own detailed README with full API tables, configuration references and
troubleshooting — see [Repository Layout](#repository-layout).

---

## Tech Stack

| Layer | Technology |
|---|---|
| Language | Java 17 (Kotlin in `fund-transfer-service`) |
| Framework | Spring Boot 3.5.5 |
| Service discovery | Spring Cloud 2025.0.0 · Netflix Eureka |
| Gateway | Spring Cloud Gateway (WebFlux, reactive) |
| Inter-service calls | `RestClient` with `@LoadBalanced` — no Feign, no WebClient |
| Messaging | Apache Kafka 3.9.1 · `spring-kafka` · 22 topics |
| Persistence | PostgreSQL 16 · Spring Data JPA / Hibernate (`ddl-auto: update`) |
| Cache / OTP state | Redis 7 · Spring Data Redis (Lua scripts for atomic OTP ops) |
| Security | Spring Security (user-service), JJWT 0.12.6, BCrypt, AES-256-GCM event crypto |
| Resilience | Resilience4j 2.3.0 (circuit breaker, retry) |
| Observability | Actuator, Micrometer + Prometheus, OpenTelemetry (OTLP), Logstash JSON logs, Grafana/Loki/Tempo |
| Front end | Vanilla HTML, CSS and JavaScript · Python 3 standard-library file server |
| Build | Maven (Maven Wrapper included in every service) |
| Containers | Docker + Docker Compose |

---

## Prerequisites

| Tool | Version | Notes |
|---|---|---|
| JDK | 17 or newer | The build targets Java 17; the `mvnw` wrapper handles the rest |
| Maven | 3.9+ | Or use the bundled `./mvnw` — no global install needed |
| Docker | 24+ | With Compose v2 (`docker compose`, not `docker-compose`) |
| Python | 3.8+ | For the web UI's static file server — standard library only, no `pip install` |
| E-mail account | — | A Gmail address plus an [app password](https://myaccount.google.com/apppasswords) |

You also need free ports: **3000** (web UI), **8080–8090** (services), **8761** (Eureka),
**9092** (Kafka), **5433** (PostgreSQL), **6379** (Redis).

---

## Quick Start

### 1. Create your environment file

The repository ships with placeholders only. Copy the example and fill in your own values:

```bash
cd banking-microservies
cp .env.example .env
```

Then generate the three secrets it needs:

```bash
echo "EVENT_CRYPTO_KEY=$(openssl rand -base64 32)"   # must decode to exactly 32 bytes
echo "JWT_SECRET=$(openssl rand -hex 32)"             # must be identical in gateway + user-service
echo "OTP_HASH_SECRET=$(openssl rand -hex 32)"
```

Paste those three values into `.env`, and set `MAIL_USERNAME` / `MAIL_PASSWORD` to your Gmail
address and app password.

### 2. Start the infrastructure

PostgreSQL and Redis come from the root compose file:

```bash
docker compose up -d
```

Kafka lives in its own compose file, along with a browser UI for inspecting topics:

```bash
docker compose -f event-bus-service/docker-compose.yml up -d
```

That gives you Kafka on `:9092` and Kafka UI on <http://localhost:8089>.

### 3. Build the services

Each service is an independent Maven project. From the repository root:

```bash
for svc in discovery-service event-bus-service core-banking-service user-service \
           account-service charge-fee-service otp-service notification-service \
           fund-transfer-service api-gateway; do
  (cd "$svc" && ./mvnw -q -DskipTests package) || echo "FAILED: $svc"
done
```

Each build produces `target/<service>-<version>.jar`.

### 4. Start the services — in order

**Order matters.** Eureka must be up before anything registers with it, and the event bus must be
up before any service publishes, because Kafka runs with auto-create disabled.

Start them in this order, each in its own terminal (or backgrounded with `&`):

```bash
# 1. Service registry — everything else registers here
cd discovery-service && java -jar target/discovery-service-*.jar

# 2. Kafka topic owner — creates all 22 topics
cd event-bus-service && java -jar target/event-bus-service-*.jar

# 3. Domain services
cd core-banking-service     && java -jar target/core-banking-service-*.jar
cd user-service             && java -jar target/user-service-*.jar
cd account-service          && java -jar target/account-service-*.jar
cd charge-fee-service       && java -jar target/charge-fee-service-*.jar
cd otp-service              && java -jar target/otp-service-*.jar
cd notification-service     && java -jar target/notification-service-*.jar
cd fund-transfer-service    && java -jar target/fund-transfer-service-*.jar

# 4. The front door
cd api-gateway && java -jar target/api-gateway-*.jar
```

Each service reads the root `.env` automatically. Every service imports it with:

```yaml
spring:
  config:
    import:
      - "optional:file:../.env[.properties]"   # the repository-root .env
      - "optional:file:.env[.properties]"      # a service-local .env, if you keep one
```

Run each service **from its own directory** — the `../.env` path is relative to the working
directory, so it resolves to the repository-root `.env`.

> **Watch out for stale shell exports.** Spring Boot gives the OS environment a *higher* priority
> than an imported config file. If your shell already exports `REDIS_PASSWORD`, `POSTGRES_PASSWORD`,
> `DB_PASSWORD`, `JWT_SECRET` or `EVENT_CRYPTO_KEY`, those values win over `.env` — and you will
> get a confusing `WRONGPASS` or `password authentication failed` error. Check with
> `env | grep -E 'REDIS_PASSWORD|POSTGRES_PASSWORD|JWT_SECRET'`. Either unset them
> (`unset REDIS_PASSWORD POSTGRES_PASSWORD`) or make sure they match your `.env`.

> **Verify as you go.** Eureka's dashboard at <http://localhost:8761> should show services
> registering within a few seconds of each start. `event-bus-service` exposes
> `GET /internal/events/health` to confirm all 22 topics exist.

### 5. Start the web UI

```bash
cd banking-web-ui && python3 serve.py
```

Open <http://localhost:3000>. Register, check your inbox for the verification link, and you are in.

### 6. Smoke test the API

The following sequence is verified end to end — it creates a user, opens an account, and moves
money through the ledger.

```bash
# Register. The answer is deliberately vague ("if this email can be registered...")
# so it cannot be used to discover which addresses exist.
curl -X POST http://localhost:8080/api/auth/register \
  -H 'Content-Type: application/json' \
  -d '{"fullName":"Ada Lovelace","email":"ada@example.com","password":"S3cret!pass"}'
# → HTTP 202

# A new user is PENDING until the e-mail is verified. With SMTP configured you
# click the link in the inbox. For a local test without SMTP, activate the user
# directly:
docker exec banking-postgres psql -U banking_admin -d user_db \
  -c "UPDATE users SET status='ACTIVE' WHERE email='ada@example.com';"

# Log in — returns a JWT valid for one hour
TOKEN=$(curl -s -X POST http://localhost:8080/api/auth/login \
  -H 'Content-Type: application/json' \
  -d '{"email":"ada@example.com","password":"S3cret!pass"}' \
  | grep -oP '"token":"\K[^"]+')

# Open an account. It comes back PENDING while the ledger validates it,
# and flips to ACTIVE within a few seconds.
curl -s -X POST http://localhost:8080/api/accounts \
  -H "Authorization: Bearer $TOKEN" -H 'Content-Type: application/json' \
  -d '{"accountType":"SAVINGS","currency":"USD"}'
# → {"accountNumber":"ACC-48649262", ..., "status":"PENDING"}

# Deposit 500. The saga writes a balanced entry to the ledger.
curl -s -X POST http://localhost:8080/api/transfers/deposit \
  -H "Authorization: Bearer $TOKEN" -H 'Content-Type: application/json' \
  -d '{"accountNumber":"ACC-48649262","amount":500.00}'

# The balance is now 500.00
curl -s http://localhost:8080/api/accounts/ACC-48649262 -H "Authorization: Bearer $TOKEN"
```

A **transfer** between two accounts runs the full saga — validate accounts, calculate the fee,
issue an OTP, e-mail it, and post the ledger entries. Without working SMTP credentials the
transfer correctly ends in `FAILED` with reason `OTP_DELIVERY_FAILED` and **both balances stay
unchanged** — which is itself a useful check that the compensating logic works.

```bash
curl -s -X POST http://localhost:8080/api/transfers \
  -H "Authorization: Bearer $TOKEN" -H 'Content-Type: application/json' \
  -d '{"fromAccountNumber":"ACC-48649262","toAccountNumber":"ACC-40953669","amount":100.00}'

curl -s http://localhost:8080/api/transfers/<transferId> -H "Authorization: Bearer $TOKEN"
```

### Quick sanity checks

```bash
# All 22 Kafka topics exist
curl -s http://localhost:8090/internal/events/health
# → {"brokerReachable":true,"allTopicsPresent":true,"missingTopics":[]}

# Every service is registered (discovery-service itself does not register)
curl -s -H 'Accept: application/json' http://localhost:8761/eureka/apps | grep -o '"name":"[A-Z-]*"'

# The gateway blocks internal paths
curl -s -o /dev/null -w '%{http_code}\n' http://localhost:8080/internal/events/topics   # → 404

# Protected endpoints need a token
curl -s -o /dev/null -w '%{http_code}\n' http://localhost:8080/api/accounts              # → 401

# The rate limiter kicks in after 10 registrations in an hour
for i in $(seq 1 12); do
  curl -s -o /dev/null -w '%{http_code} ' -X POST http://localhost:8080/api/auth/register \
    -H 'Content-Type: application/json' \
    -d "{\"fullName\":\"T$i\",\"email\":\"t$i@example.com\",\"password\":\"S3cret!pass\"}"
done
# → 400 400 400 400 400 400 400 400 400 429 429 429
```

---

## Running the Services

### Running one service in isolation

Every service ships a Maven Wrapper, so a global Maven install is optional:

```bash
cd user-service
./mvnw spring-boot:run
```

Or run the packaged jar:

```bash
cd user-service
./mvnw -DskipTests package
java -jar target/user-service-*.jar
```

### The `local` profile

`api-gateway` ships an `application-local.yml` that bypasses Eureka and points directly at
`127.0.0.1:8081/8082/8083`, with Eureka disabled. It is useful when you are working on the
gateway alone:

```bash
cd api-gateway && ./mvnw spring-boot:run -Dspring-boot.run.profiles=local
```

> The `local` profile is defined for the gateway **only**. The other services do not have one —
> running them with `SPRING_PROFILES_ACTIVE=local` has no effect.

### Health and metrics

Every service exposes Actuator on a **separate port** (the main port stays free for business
traffic):

| Service | Health | Metrics |
|---|---|---|
| api-gateway | <http://localhost:18080/actuator/health> | <http://localhost:18080/actuator/prometheus> |
| account-service | <http://localhost:18081/actuator/health> | <http://localhost:18081/actuator/prometheus> |
| fund-transfer-service | <http://localhost:18082/actuator/health> | <http://localhost:18082/actuator/prometheus> |
| user-service | <http://localhost:18083/actuator/health> | <http://localhost:18083/actuator/prometheus> |
| core-banking-service | <http://localhost:18084/actuator/health> | <http://localhost:18084/actuator/prometheus> |
| charge-fee-service | <http://localhost:18085/actuator/health> | <http://localhost:18085/actuator/prometheus> |
| otp-service | <http://localhost:18086/actuator/health> | <http://localhost:18086/actuator/prometheus> |
| notification-service | <http://localhost:18087/actuator/health> | <http://localhost:18087/actuator/prometheus> |
| event-bus-service | <http://localhost:18090/actuator/health> | <http://localhost:18090/actuator/prometheus> |
| discovery-service | <http://localhost:18761/actuator/health> | <http://localhost:18761/actuator/prometheus> |

### Logging

All services write **structured JSON** logs to `~/banking-logs/<service>.json`, rolling at 20 MB
with 3 generations and a 200 MB total cap. Override the directory with `BANKING_LOG_DIR`.

---

## Configuration

### Environment variables

The root `.env` holds the configuration shared across the platform. `.env.example` documents every
variable; these are the ones that matter most:

| Variable | Used by | Purpose |
|---|---|---|
| `POSTGRES_PASSWORD` | `docker-compose.yml` | Password for the `banking_admin` Postgres role |
| `REDIS_PASSWORD` | compose, gateway, user-service, otp-service | Redis auth, and the gateway's rate-limit counters |
| `DB_URL` | all JPA services | JDBC URL — override to point a service at a different database |
| `DB_USERNAME` / `DB_PASSWORD` | all JPA services | Postgres credentials |
| `JWT_SECRET` | **gateway + user-service** | Signing key. Must be **identical** in both, at least 32 characters |
| `EVENT_CRYPTO_KEY` | **otp + notification + user-service** | AES-256-GCM key. Must decode to exactly 32 bytes and be **identical** in all three |
| `OTP_HASH_SECRET` | otp-service | HMAC key used to hash OTPs before storing them |
| `MAIL_USERNAME` / `MAIL_PASSWORD` | notification-service | SMTP login — use a Gmail *app password*, not your account password |
| `KAFKA_BOOTSTRAP_SERVERS` | all Kafka clients | Defaults to `localhost:9092` |
| `EUREKA_URL` | all Eureka clients | Defaults to `http://localhost:8761/eureka` |
| `ACCOUNT_DEMO_BALANCE` | account-service | Opening balance for new accounts — dev only, keep `0` for anything real |
| `BANKING_LOG_DIR` | all services | Where JSON logs are written |
| `GRAFANA_ADMIN_PASSWORD` | monitoring compose | Grafana admin password |

> **The three shared secrets.** `JWT_SECRET`, `EVENT_CRYPTO_KEY` and `OTP_HASH_SECRET` are not
> per-service secrets — the services that use them must agree on the same value. If the gateway
> and `user-service` disagree on `JWT_SECRET`, the gateway rejects every token the user-service
> issues and every authenticated request fails with `401`.

### Where configuration lives

| Location | Purpose |
|---|---|
| `.env` (root) | Shared secrets and connection settings. Git-ignored. |
| `.env.example` (root) | The documented template. Safe to commit. |
| `postgres-init/*.sql` | Runs once on a fresh Postgres volume — creates the five databases |
| `*/src/main/resources/application.yaml` | Per-service defaults, all overridable by environment variable |
| `*/src/main/resources/application-local.yml` | Gateway only — Eureka-free local development |
| `*/.env.example` | Gateway's own template |

**How the values reach a service.** Each service imports the root `.env` relative to its own
working directory:

```yaml
spring:
  config:
    import:
      - "optional:file:../.env[.properties]"
      - "optional:file:.env[.properties]"
```

Because the OS environment outranks an imported file, an exported shell variable wins over `.env`.
That is deliberate — it lets you override a single value for one run — but it is the most common
reason a fresh setup fails to authenticate.

### A note on the per-service READMEs

The individual service READMEs mention a `run-all.sh` script and a `run.env` file. **Those do not
exist in this repository** — they belonged to the upstream project this code was derived from. Use
the manual startup order in [Quick Start](#quick-start) instead; the scripts' comments are still
useful reading for *why* the order matters.

---

## API Overview

The gateway is the only public surface. Everything the browser calls goes through it.

### Public routes

| Method | Path | Auth | Purpose |
|---|---|---|---|
| `POST` | `/api/auth/register` | Public | Create a user account (rate limited to 10/hour) |
| `POST` | `/api/auth/verify` | Public | Verify an e-mail address (rate limited to 20/hour) |
| `POST` | `/api/auth/login` | Public | Exchange credentials for a JWT (rate limited to 30/5min) |
| `GET` | `/api/accounts` | Bearer | List the caller's accounts |
| `POST` | `/api/accounts` | Bearer | Open a savings or current account |
| `GET` | `/api/accounts/{accountNumber}` | Bearer | Fetch one account |
| `POST` | `/api/transfers` | Bearer | Start a transfer (may require OTP confirmation) |
| `POST` | `/api/transfers/deposit` | Bearer | Deposit into an account |
| `POST` | `/api/transfers/withdraw` | Bearer | Withdraw from an account |
| `POST` | `/api/transfers/{transferId}/confirm` | Bearer | Confirm a transfer with the OTP code |
| `POST` | `/api/transfers/{transferId}/resend-otp` | Bearer | Resend the OTP (30s cooldown) |
| `GET` | `/api/transfers/{transferId}` | Bearer | Poll a transfer's status |
| `GET` | `/api/transfers` | Bearer | List the caller's transfers |

### Internal routes — never reachable from outside

Every `/internal/**` path is answered with **404** by the gateway. These are for service-to-service
calls only:

| Path | Service | Purpose |
|---|---|---|
| `/internal/users/{id}` | user-service | Look up a user |
| `/internal/accounts`, `/internal/accounts/{n}` | account-service | Account lookups and validation |
| `/internal/cbs/**` | core-banking-service | Ledger accounts, balances, transfers, deposits, withdrawals |
| `/internal/fees/calculate` | charge-fee-service | Calculate a transfer fee |
| `/internal/otp/send`, `/internal/otp/verify` | otp-service | Issue and verify one-time passwords |
| `/internal/notifications/email`, `/internal/notifications/templated` | notification-service | Send an e-mail |
| `/internal/events/{health,topics,dead-letters}` | event-bus-service | Inspect the Kafka topic catalog |

### How identity works

The gateway validates the JWT and injects trusted identity headers — `X-User-Id`, `X-User-Email`,
`X-User-Roles`. It **strips any incoming headers with those names** before routing, so a client
cannot impersonate another user by setting them by hand. The domain services trust those headers
and do no authentication of their own.

Error responses from the gateway use a consistent shape:

```json
{ "status": 401, "error": "Unauthorized", "message": "..." , "timestamp": "..." }
```

---

## Kafka Topics

`event-bus-service` owns the topic catalog. On startup it creates all **22 topics** — 11 business
topics plus a `.DLT` dead-letter topic for each — with 3 partitions, replication factor 1, and a
retention of 7 days (2 hours for `otp.events`, because an OTP only lives 5 minutes).

| Topic | Kind | Contents |
|---|---|---|
| `account.commands` | commands | `ValidateTransfer`, `ValidateCashAccount` |
| `account.events` | events | `TransferValidated`, `TransferRejected`, `CashAccountValidated`, `CashAccountRejected`, `AccountOpened` |
| `fee.commands` | commands | `CalculateFee` |
| `fee.events` | events | `FeeCalculated` |
| `otp.commands` | commands | `IssueOtp` |
| `otp.events` | events | `OtpIssued` (the code is AES-encrypted) |
| `cbs.commands` | commands | `CreateLedgerAccount`, `CheckBalance`, `Withdraw`, `Deposit`, `Reversal` |
| `cbs.events` | events | `LedgerAccountCreated`, `BalanceChecked`, `BalanceRejected`, `WithdrawCompleted/Failed`, `DepositCompleted/Failed`, `ReversalCompleted/Failed` |
| `transfer.events` | events | `TransferCompleted`, `TransferFailed`, `TransferReversed`, `ReconciliationRequired` |
| `user.events` | events | `UserRegistered`, `EmailVerificationRequested` |
| `notification.events` | events | `OtpDeliveryFailed` |

Each has a `<topic>.DLT` counterpart holding messages that failed after every retry.

**Why the event bus must start first.** The broker runs with `KAFKA_AUTO_CREATE_TOPICS_ENABLE:
"false"`. Without `event-bus-service` no topic exists and *every* event publish fails.

### The transactional outbox

Services that write to their own database and then publish an event do it through a **transactional
outbox**: the event row is written in the same transaction as the business data, and a relay
process publishes it to Kafka and marks it sent. This is what prevents a committed database
transaction from losing its event — or publishing it twice. Consumers de-duplicate through a
`processed_events` table.

---

## Databases

A single PostgreSQL instance on port **5433** holds one database per service. The databases
themselves are created by `postgres-init/01-create-databases.sql`, which the Postgres container
runs once on a fresh volume; the tables inside them are created automatically by Hibernate
(`ddl-auto: update`) — there are no SQL migration files.

| Database | Owner | Main tables |
|---|---|---|
| `user_db` | user-service | `users`, `email_verification_tokens`, `outbox_events` |
| `account_db` | account-service | `accounts`, `outbox_events`, `processed_events` |
| `fund_transfer_db` | fund-transfer-service | `transfers`, `outbox_events`, `processed_events` |
| `cbs_db` | core-banking-service | `ledger_accounts`, `ledger_entries`, `ledger_transactions`, `command_results`, `transit_settlements`, `outbox_events` |
| `notification_db` | notification-service | `notification_logs`, `processed_events` |

`core-banking-service` also creates two **system accounts** at startup: `FEE-INCOME` (where fees
land) and `TRANSFER-TRANSIT` (where money sits while a transfer is in flight).

> The `accounts` table holds **no balance column** — balances live only in the ledger. That is
> deliberate: one source of truth for money.

### Redis

Redis (port **6379**) holds no business data. It stores OTP state (`otp:` prefixed keys, managed
with Lua scripts for atomic issue-and-verify), OTP command idempotency markers, the registration
e-mail rate limiter, and the gateway's rate-limit counters.

---

## The Web UI

`banking-web-ui` is **not** a Node project and has **no build step**. It is eight HTML pages, four
vanilla-JS modules, one CSS file, and a 33-line Python standard-library server.

```bash
cd banking-web-ui
python3 serve.py          # defaults to port 3000
python3 serve.py 3000     # explicit port
```

Open <http://localhost:3000>. The server adds the `.html` extension automatically (so `/login`
serves `login.html`) and sends `Cache-Control: no-store`, so a refresh always shows your latest
edits.

| Page | Purpose |
|---|---|
| `register.html` | Create an account |
| `verify.html` | Confirm the e-mail address from the link |
| `login.html` | Sign in |
| `dashboard.html` | Accounts and balances |
| `cash.html` | Deposit and withdraw |
| `transfer.html` | Start a transfer, enter the OTP, watch it progress |
| `history.html` | Past transfers and their outcomes |

The session lives in `sessionStorage` — it disappears when the tab closes. There is no refresh token
and no server-side session. The UI talks to **the gateway only**; it never calls a domain service
directly, and it does not appear in the Eureka registry.

---

## Observability

### Built in

Every service exposes Actuator on its own management port with `health`, `info` and `prometheus`
endpoints, emits Micrometer metrics with percentile histograms, exports OpenTelemetry traces over
OTLP to `http://localhost:4318/v1/traces`, and writes Logstash-format JSON logs.

### The monitoring stack

`event-bus-service/docker-compose.monitoring.yml` brings up the full Grafana stack. It
references the `kafka` service for the exporter, so it must be started **together with** the
Kafka compose file:

```bash
docker compose \
  -f event-bus-service/docker-compose.yml \
  -f event-bus-service/docker-compose.monitoring.yml \
  up -d
```

| Component | Port | Purpose |
|---|---|---|
| Grafana | 3001 | Dashboards and log search (admin / `GRAFANA_ADMIN_PASSWORD`) |
| Prometheus | 9090 | Metrics, scraping every service every 15s |
| Loki | 3100 | Log aggregation (reads `~/banking-logs`) |
| Tempo | 3200 | Trace storage (OTLP gRPC 4317, HTTP 4318) |
| Alloy | 12345 | Log shipper from `~/banking-logs` into Loki |
| kafka-exporter | — | Kafka broker metrics |

Five dashboards are pre-provisioned in Grafana: **platform**, **communication**, **traffic**,
**logs**, and **traces**.

### Logs without the stack

All services write JSON to `~/banking-logs/<service>.json` regardless of whether Grafana is
running, so you can always tail a file:

```bash
tail -f ~/banking-logs/fund-transfer-service.json | jq .
```

---

## Testing

Each service has its own test suite:

```bash
cd core-banking-service && ./mvnw test
```

Or across the whole platform:

```bash
for svc in discovery-service event-bus-service core-banking-service user-service \
           account-service charge-fee-service otp-service notification-service \
           fund-transfer-service api-gateway; do
  (cd "$svc" && ./mvnw -q test) || echo "FAILED: $svc"
done
```

Integration tests that need PostgreSQL, Redis or Kafka will fail without those running. Note that
`user-service` has an `app.registration.verification-required` flag — set it to `false` to let
tests skip the e-mail verification step.

> All ten suites currently pass against the infrastructure in this repository
> (`docker compose up -d` plus the Kafka compose file).

---

## Repository Layout

This is a **monorepo of git submodules**. Each service is an independent repository with its own
`pom.xml`, own README, and its own release cycle.

```
banking-microservies/
├── README.md                  ← you are here
├── .env.example               ← the documented environment template
├── .env                       ← your local values (git-ignored)
├── .gitmodules                ← the service directories are git submodules
├── docker-compose.yml         ← PostgreSQL 16 + Redis 7
├── postgres-init/             ← Postgres init scripts, mounted into the container
│   └── 01-create-databases.sql   (creates the five per-service databases)
│
├── docs/                      ← project documentation assets
│   └── architecture.gif          (the animated diagram used above)
│
├── discovery-service/         ← Eureka registry        :8761
├── event-bus-service/         ← Kafka topic catalog   :8090
│   ├── docker-compose.yml           (Kafka + Kafka UI)
│   ├── docker-compose.monitoring.yml (Grafana stack)
│   └── monitoring/                  (Prometheus, Loki, Tempo, Alloy, Grafana dashboards)
│
├── core-banking-service/      ← double-entry ledger   :8084
├── user-service/              ← identity              :8083
├── account-service/           ← accounts              :8081
├── charge-fee-service/        ← fees                  :8085
├── otp-service/               ← one-time passwords    :8086
├── notification-service/      ← e-mail delivery       :8087
├── fund-transfer-service/     ← saga orchestrator     :8082
├── api-gateway/               ← front door            :8080
│
└── banking-web-ui/            ← static front end      :3000
```

Each service follows the same internal shape:

```
<service>/
├── README.md                        ← full API, config and troubleshooting reference
├── pom.xml
├── mvnw / mvnw.cmd                  ← Maven Wrapper
└── src/main/java/com/banking/<service>/
    ├── <service>/                   ← controllers, entities, clients, messaging, config
    └── ...
```

---

## Troubleshooting

| Symptom | Cause | Fix |
|---|---|---|
| `POSTGRES_PASSWORD:?Set POSTGRES_PASSWORD in .env` | `.env` missing or incomplete | `cp .env.example .env` and fill it in |
| `WRONGPASS invalid username-password pair` | A stale `REDIS_PASSWORD` in your **shell** is overriding `.env` | `unset REDIS_PASSWORD`, or make it match `.env` |
| `password authentication failed for user "banking_admin"` | A stale `POSTGRES_PASSWORD` / `DB_PASSWORD` in your shell, or the volume was created with a different password | `unset POSTGRES_PASSWORD DB_PASSWORD`; if the volume is stale, `docker compose down -v && docker compose up -d` |
| `database "cbs_db" does not exist` | The Postgres volume predates the init script | `docker compose down -v && docker compose up -d` — the init script creates all five databases on a fresh volume |
| `EVENT_CRYPTO_KEY must be 32 bytes` / `is not valid Base64` | The key is not valid base64 of exactly 32 bytes | `openssl rand -base64 32` — the placeholder in `.env.example` is intentionally invalid so it fails loudly |
| Every event publish fails | Kafka topics do not exist | Start `event-bus-service` — it creates all 22. Check `GET /internal/events/health` |
| `No servers available for service: ...` (HTTP 503) | The registry cache is still empty after a restart | Wait a few seconds; the gateway re-fetches every 5s. If it persists, is Eureka up on 8761? |
| Every authenticated request returns 401 | `JWT_SECRET` differs between gateway and user-service | Make them byte-identical in `.env` and restart both |
| OTPs never arrive by e-mail | SMTP credentials are wrong, or no app password | Use a Gmail **app password**, not your account password |
| A transfer ends in `FAILED` / `OTP_DELIVERY_FAILED` | The OTP could not be e-mailed — no working SMTP credentials | Expected without SMTP. The balances are correctly left unchanged |
| `MailHealthIndicator` never turns green | Deliberate — it opens a real SMTP connection on every health check | Nothing to do; the indicator is disabled on purpose |
| `Web server failed to start. Port 8086 was already in use` | A previous instance of that service is still running | Stop it: `ps -eo pid,args \| grep "java -jar target/"` and `kill` the pid |
| A service reports `{"description":"Remote status from Eureka server","status":"DOWN"}` | The Eureka lease had not been renewed yet — usually a few seconds after start, and it can recur later | Check the registry itself: `curl -H 'Accept: application/json' http://localhost:8761/eureka/apps`. If it says `UP`, the service is fine — restart that one service |
| Ports already in use | Another process holds a port | `ss -ltn \| grep :8080` and stop it, or change `server.port` |
| The web UI says *"Cannot reach the server"* | The gateway is not running on 8080 | Start `api-gateway` |
| `relation "transfers" violates check constraint` | A stale constraint from an earlier schema | Drop and recreate `fund_transfer_db` |
| Kafka container restarts in a loop | A previous volume has a bad `meta.properties` | `docker compose -f event-bus-service/docker-compose.yml down -v` and start again |

---

## Security Notes

**This repository is a development project.** Read this before running it anywhere that matters.

1. **Secrets are not committed.** The `.env` file is git-ignored, and `.env.example` contains only
   placeholders. Every service reads its secrets from the environment with **no fallback default** —
   if `DB_PASSWORD` or `JWT_SECRET` is missing, the service fails to start rather than silently
   using a well-known value. The `EVENT_CRYPTO_KEY` placeholder is deliberately *not* valid base64,
   so a forgotten key fails immediately and loudly instead of silently weakening the encryption.
2. **Earlier commits in this repository's history contained working credentials.** Removing them
   from the current tree does not remove them from git history. If this repository has ever been
   pushed to a remote, **treat every credential that was ever in it as compromised and rotate it**:
   the Postgres password, the Redis password, the JWT secret, the event crypto key, the OTP hash
   secret, and the SMTP app password. Rotating them means changing the actual Postgres, Redis and
   mailbox credentials — a new value in `.env` only affects this checkout.
3. **The JWT secret is shared** between the gateway and `user-service`. Anyone holding it can mint
   valid tokens. Generate it fresh: `openssl rand -hex 32`.
4. **`notification-service` holds the SMTP credentials** and is the only service that decrypts OTP
   codes and e-mail verification tokens.
5. **E-mail verification can be disabled** with `app.registration.verification-required: false`.
   This exists for automated tests — **do not leave it off** outside a test environment.
6. **`ACCOUNT_DEMO_BALANCE` seeds new accounts with a starting balance.** It exists so transfers can
   be tested without a funding step. Keep it at `0` for anything real.
7. **The gateway is the only authentication point.** Domain services trust the `X-User-*` headers it
   injects and do no authentication themselves. They must never be exposed directly — they bind to
   the host only, and the gateway blocks all `/internal/**` paths.
8. **The e-mail rate limiter fails open.** If Redis is unreachable, registration is not rate
   limited. Monitor Redis availability.
9. **The Postgres init scripts live in `postgres-init/`.** The original `postgres/init/` directory
   belonged to the upstream project; the compose file mounts `postgres-init/` instead, and the old
   path is gone.

   Only `01-create-databases.sql` remains there. It creates the five databases; the tables inside
   them are created and kept in shape by Hibernate (`ddl-auto: update`) in each service. The
   upstream repair scripts (`02-notification-processed-events.sql`, `03-fund-transfer-schema.sql`,
   `04-repair-schema.sql`) no longer exist and nothing runs them. A database carried over from
   before the saga flow may still hold a stale check constraint — the affected services' READMEs
   say which column to drop.

---

## Contributing

1. Each service is a separate repository — work inside the submodule you are changing.
2. Follow the existing package layout: `com.banking.<service>` with `controller`, `service`,
   `entity`, `client`, `messaging`, `config` and `config`-adjacent concerns kept separate.
3. **Never commit a secret.** Use the environment variables in `.env.example`; if a new setting is
   needed, add a placeholder default to `.env.example` and document it in the service's README.
4. Update the service's own README when you change its API or configuration — the per-service
   READMEs are the detailed reference, and this root README only summarises.
5. Keep the event contract in sync: a change to a Kafka payload needs matching updates on both the
   publishing and consuming side, and a topic change needs an entry in `TopicCatalog`.
6. Run the tests before you push: `./mvnw test` in the service you touched.

---

## License

**This project has no license.** It is not open source, and it is not public domain.

No permission is granted to copy, modify, distribute or use this software, in whole or in part,
for any purpose. The copyright is retained by the authors. If you received a copy of it, you may
not use it.

This matters in practice: without a license, the default legal position applies and nobody —
including you, in six months — has permission to reuse, fork or redistribute this code. If you
intend to publish it, share it, or build on it, add a `LICENSE` file and point this section at
it. MIT is the usual choice for a project of this kind.

The upstream project this code was derived from carried its own license; check the history of the
individual service repositories if you need to know what it was.

---

Built with Spring Boot, Spring Cloud, Kafka, PostgreSQL, Redis, Docker and a lot of care.
