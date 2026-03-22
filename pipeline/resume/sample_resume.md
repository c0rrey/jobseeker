# Alex Morgan

Austin, TX | alex.morgan@email.com | 555-555-0123

---

## Summary

Senior software engineer with 10+ years of experience building distributed systems and data pipelines at scale. Proven track record delivering high-impact products across social, gaming, and independent consulting contexts. Comfortable operating at the intersection of backend engineering, platform architecture, and applied AI tooling.

---

## Core Skills

**Languages & Frameworks**
Python, TypeScript, SQL, Bash, FastAPI, Django, React

**Data & Infrastructure**
PostgreSQL, SQLite, Redis, Kafka, Docker, Kubernetes, AWS (EC2, S3, Lambda, RDS)

**Engineering Practices**
Distributed systems design, API design, CI/CD pipelines, test-driven development, observability, async programming

---

## Experience

### Senior Software Engineer — Major Social Platform
*2019 – Present | Austin, TX*

- Designed and maintained a real-time data ingestion pipeline processing 500K+ events per second, reducing end-to-end latency by 40%.
- Led a cross-functional team of 6 engineers to migrate a monolithic service to a microservices architecture, improving deployment cadence from monthly to daily.
- Built an internal ML feature-serving platform used by 12 downstream product teams, cutting model deployment time from 3 weeks to 2 days.
- Architected a multi-tenant job queue system backed by Redis Streams, handling 2M+ jobs per day with sub-second SLA compliance at the 99th percentile.
- Drove adoption of observability best practices (structured logging, distributed tracing, alerting), reducing mean time to detection (MTTD) by 60%.

### Software Engineer — Global Gaming Platform
*2015 – 2019 | Remote*

- Developed a fraud detection microservice that analyzed 1M+ real-money transactions per day, flagging suspicious sessions with 94% precision and reducing chargebacks by 35%.
- Rebuilt the player analytics backend from scratch using event-sourcing principles, enabling retroactive computation of game state for audit and debugging.
- Optimized a critical PostgreSQL query responsible for leaderboard generation, reducing P99 latency from 4.2 s to 180 ms through index redesign and query rewriting.
- Collaborated with the security team to implement rate limiting and bot-detection middleware, blocking an estimated 12K automated account registrations per month.

---

## Independent Projects

### Multi-Agent Orchestration Framework
*2023 – Present*

- Designed a framework for coordinating parallel AI agent workflows with dependency-aware task scheduling, checkpoint verification, and structured output contracts.
- Implemented a task graph execution engine that spawns concurrent subagents, enforces hard gates between phases, and surfaces findings to a human reviewer before proceeding.
- Wrote a CLI tool for task lifecycle management (create, claim, update, close) backed by a local YAML store with full audit trail.

### Knowledge Management System
*2022 – 2023*

- Built a personal knowledge base tool that indexes markdown notes, extracts semantic relationships, and surfaces relevant context during writing sessions.
- Integrated vector similarity search using sentence-transformer embeddings stored in a lightweight SQLite extension, enabling sub-100 ms nearest-neighbor lookup over 50K+ note fragments.
- Shipped a VS Code extension companion that calls the local REST API and injects retrieved context into the editor sidebar on demand.
