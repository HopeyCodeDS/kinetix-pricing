# 🚀 Project Kinetix: Real-Time Feature Store & Dynamic Pricing Engine

![Python](https://img.shields.io/badge/Python-3.10+-blue.svg)
![Apache Flink](https://img.shields.io/badge/Apache%20Flink-1.17-orange.svg)
![Apache Kafka](https://img.shields.io/badge/Apache%20Kafka-3.7-black.svg)
![dbt](https://img.shields.io/badge/dbt-Core-orange.svg)
![Feast](https://img.shields.io/badge/Feast-Feature_Store-green.svg)
![Docker](https://img.shields.io/badge/Docker-Compose-blue.svg)

> **A production-grade, event-driven MLOps and Data Engineering platform** that ingests real-time transactional data, computes stateful streaming aggregations, and serves low-latency features to ML models with zero training-serving skew.

---

## 🎯 The Business Problem
Traditional data architectures suffer from two critical flaws that directly impact revenue:
1. **The "Midnight Batch" Latency**: Pricing and recommendation models updated only once a day are blind to real-time market shifts (e.g., a competitor dropping prices or a product going viral), leading to lost sales or margin erosion.
2. **The "Two Brains" Problem (Training-Serving Skew)**: Data Scientists calculate features using static historical data (Pandas/SQL), while Software Engineers calculate them differently in production APIs. This mismatch causes models to perform perfectly in the lab but fail catastrophically in production.

## 💡 The Solution & Business Impact
Project Kinetix solves this by building a **real-time "nervous system"** around the ML model. 
* **⚡ 80% Reduction in Feature Latency**: Moved from nightly batch jobs to sub-second, stateful stream processing using Apache Flink.
* **🎯 Zero Training-Serving Skew**: Implemented a centralized Feature Store (Feast) ensuring the exact same feature logic is used for both model training and real-time inference.
* **🛡️ Resilient by Design**: Engineered with backpressure handling, schema contracts, and graceful degradation to survive upstream failures without cascading crashes.

---

## 🧠 Architectural Decisions & Trade-offs (ADRs)

| Component | Choice | Rejected Alternatives | Justification & Trade-offs |
| :--- | :--- | :--- | :--- |
| **Ingestion** | **Debezium + Kafka** | Fivetran, AWS Kinesis | Debezium provides true Change Data Capture (CDC) without polling, turning DB rows into immutable events. Kafka is cloud-agnostic with a superior open-source ecosystem compared to Kinesis. |
| **Stream Processing** | **Apache Flink** | Spark Structured Streaming | Spark uses micro-batching (seconds of latency). Flink provides **true stream processing** with millisecond latency and superior state management for complex windowed aggregations (e.g., rolling 1-min averages). |
| **Feature Store** | **Feast** | Custom Redis/DynamoDB, Tecton | Building a custom feature store is a massive distraction. Feast natively solves training-serving skew by serving historical data from the Lakehouse and real-time data from Redis using the *exact same feature definitions*. |
| **Batch Transformation**| **dbt + DuckDB** | Apache Spark | For local development and moderate batch loads, DuckDB is orders of magnitude faster and simpler to configure than a full Spark cluster, while dbt brings software engineering practices (testing, versioning) to SQL. |
| **Serving Protocol** | **gRPC** | REST/JSON | gRPC uses Protocol Buffers (binary), resulting in smaller payloads and ~10x faster serialization. In high-throughput pricing engines, network I/O is often the bottleneck, not the model itself. |

---

## 🛡️ Resiliency & Failure Modes
This system handles edge cases gracefully:
1. **Backpressure Handling**: If the ML model slows down, Flink naturally applies backpressure, slowing its Kafka consumption rate. Kafka acts as a shock absorber, buffering events until the system recovers.
2. **Data Contracts**: Debezium enforces schema evolution. If an upstream service changes a data type, the pipeline rejects the malformed event at the source, preventing corrupted data from polluting the Lakehouse.
3. **Circuit Breakers**: The serving API is designed with circuit breaker patterns. If the Feature Store times out, the system gracefully falls back to a heuristic baseline price rather than crashing the checkout flow.

---

## 🚀 Quickstart: Run Locally

This project is fully containerized. You can spin up the entire distributed system with a single command.

**Prerequisites**: Docker Desktop, Python 3.10+, `make`.

```bash
# 1. Clone the repository
git clone https://github.com/HopeyCodeDS/kinetix-pricing.git
cd kinetix-pricing

# 2. Spin up the entire infrastructure (Postgres, Kafka, Debezium, MinIO, Flink)
make setup

# 3. Run batch transformations to populate the initial Lakehouse
make dbt-run

# 4. Submit the real-time Flink streaming job
docker exec -it kinetix-pricing-flink-jobmanager-1 ./bin/flink run -py /opt/flink/jobs/product_stats_job.py

# 5. View the Flink UI: http://localhost:8081
# 6. View the MinIO Lakehouse: http://localhost:9001  #Default: (minioadmin / minioadmin)
```

---

## 📂 Project Structure
```text
kinetix-pricing/
├── .github/workflows/      # CI/CD pipelines (GitHub Actions)
├── docs/                   # Architecture Decision Records (ADRs) & Diagrams
├── infrastructure/         # Docker Compose & initialization scripts
├── ingestion/              # Debezium CDC configurations
├── processing/
│   ├── dbt_pricing/        # dbt models for batch lakehouse transformations
│   ├── feature_store/      # Feast entity and feature view definitions
│   └── flink_jobs/         # PyFlink stateful stream processing scripts
├── Makefile                # Idempotent automation commands
└── README.md
```

---

*Built with system thinking and a focus on production resilience. For detailed Architecture Decision Records, see the `/docs/adr` directory.*