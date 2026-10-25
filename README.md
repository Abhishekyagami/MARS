# Mars Cosmetics: Order & Inventory Sync Service

Design for a service that keeps orders and stock in sync across four sales channels (own website, Nykaa, Flipkart,
Blinkit) and one warehouse, and pushes every confirmed order to Microsoft Business Central exactly once.

**Core idea:** PostgreSQL is the single source of truth. Stock is reserved with one conditional `UPDATE` under a row
lock, so two channels can never take the same unit. A velocity-based buffer hides the last units on slow channels
first, which keeps overselling under 0.5%. Every change goes through a transactional outbox, so no order or stock
update is lost when a channel or the ERP is down.

## Contents

| File | What |
|---|---|
| [DESIGN.md](DESIGN.md) | The design document: architecture, data model, order ingestion, inventory consistency (last-unit race), stock push, failure handling, observability, scope, trade-offs, stretch A (second warehouse), assumptions, AI usage. |
| [architecture.png](architecture.png) | Architecture diagram (image). |
| [architecture.mmd](architecture.mmd) | Architecture diagram (Mermaid source). |

**Stack:** Node.js / NestJS, PostgreSQL (RDS), Redis (ElastiCache), AWS ECS Fargate. Estimated cost about ₹29k/month.
