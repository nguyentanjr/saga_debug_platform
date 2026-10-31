# Saga Debug Platform

A passive observer for Saga transactions in microservice systems. It reads the messages
services already exchange on Kafka, reconstructs each Saga execution from a declared workflow
specification, detects anomalies, and diagnoses the likely cause with the evidence behind it.
Observed systems are not modified: integration is done with YAML files on the observer side.

This repository is the prototype described in the accompanying paper.

## Layout

All code is under `saga-debug-platform/`.

| Path | Contents |
|---|---|
| `correlation-service/` | The observer: ingestion, execution reconstruction, detectors, diagnosis, REST API |
| `dashboard/` | Angular dashboard |
| `declarations/` | Source mappings (`ingestion/`) and workflow specifications (`workflows/`) for the evaluated systems |
| `order-service/`, `payment-service/`, `inventory-service/`, `saga-events/`, `demo-scenarios/` | Demo system used by the controlled evaluation suite |
| `evaluate.py` | Controlled evaluation suite (28 scenarios, 63 runs) |
| `tools/pm4py_real_baseline.py`, `pm4py_baseline.py` | PM4Py conformance-checking baselines |
| `benchmark/` | Observer load benchmark |

## Requirements

Java 17, Maven, Docker with Compose, and Python 3 with `requests`.
Node.js is needed only for the dashboard.

## Build and test

```bash
cd saga-debug-platform
mvn -B test
```

The tests need no Docker. They include the Saga shape matrix
(`correlation-service/.../service/SagaShapeMatrixTest.java`) and the replay of 44 captured runs of
external systems in four delivery orders
(`correlation-service/.../integration/RealProjectIntegrationTest.java`).

## Run the controlled evaluation

The container images copy the built jars, so package first. The ingest token can be any value,
but must be the same for the stack and for `evaluate.py`.

```bash
cd saga-debug-platform
mvn -B package -DskipTests
export CORRELATION_INGEST_TOKEN=any-test-value
docker compose up -d --build
python evaluate.py --full      # about 1.5 hours; --list shows the scenarios
```

The observer API is at `http://localhost:8084`.

## Dashboard

```bash
cd saga-debug-platform/dashboard
npm install
npx ng serve --port 4200
```

## Observing another system

Without changing that system, add to `declarations/`:

- a source mapping (`ingestion/*.yaml`) saying where the event id, execution key, event type and
  time are found in its messages;
- a workflow specification (`workflows/*.yaml`) declaring its steps, compensations, timeouts and
  transitions.

The files already in that directory are worked examples for Eventuate Tram, FTGO, Hotel, KMS, SPM
and ShopSaga.
