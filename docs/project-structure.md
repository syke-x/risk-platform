# Crude Oil VaR Risk Platform — Project Structure

> **Build tool**: Maven (single-module to start; promote to multi-module when teams diverge)
> **Root package**: `com.riskplatform`
> **Java version**: 21

---

## Full Directory Tree

```
risk-platform/
│
├── docs/                                   # All learning and design documents
│   ├── Master-prompt.txt
│   ├── Phase_1.txt  ...  Phase_6.txt
│   ├── progress.md                         # Session progress tracker (this project)
│   └── project-structure.md               # This file
│
├── src/
│   ├── main/
│   │   ├── java/
│   │   │   └── com/riskplatform/
│   │   │       │
│   │   │       ├── RiskPlatformMain.java           # Entry point — wires all subsystems
│   │   │       │
│   │   │       ├── domain/                         # Pure domain model — no dependencies
│   │   │       │   ├── Position.java               # record: a specific trade
│   │   │       │   ├── Instrument.java             # record: a tradeable product
│   │   │       │   ├── InstrumentType.java         # sealed interface: Futures | Physical | Swap
│   │   │       │   ├── ForwardCurve.java           # record: 60-point monthly price curve
│   │   │       │   ├── SimulationResult.java       # record: single Monte Carlo P&L trial
│   │   │       │   ├── VarResult.java              # record: final VaR/CVaR output
│   │   │       │   ├── CrudeGrade.java             # enum: BRENT, WTI, BASRAH_LIGHT ...
│   │   │       │   └── Exchange.java               # enum: ICE, CME, OTC
│   │   │       │
│   │   │       ├── ingestion/                      # Kafka consumer — position feed
│   │   │       │   ├── PositionKafkaConsumer.java  # Kafka poll loop, offset management
│   │   │       │   ├── PositionEventHandler.java   # Disruptor event handler — writes to store
│   │   │       │   └── PositionDeserializer.java   # Kafka deserializer (JSON → Position)
│   │   │       │
│   │   │       ├── store/                          # Off-heap position state (Chronicle Map)
│   │   │       │   ├── PositionStore.java          # CRUD on live portfolio positions
│   │   │       │   ├── InstrumentStore.java        # Read-only reference data cache
│   │   │       │   └── serialization/
│   │   │       │       ├── PositionMarshaller.java     # Position ↔ bytes (Chronicle Map)
│   │   │       │       └── InstrumentMarshaller.java   # Instrument ↔ bytes
│   │   │       │
│   │   │       ├── marketdata/                     # Market data management
│   │   │       │   ├── ForwardCurveManager.java    # Loads, caches, refreshes curves
│   │   │       │   ├── CorrelationMatrixLoader.java # Loads from file or DB
│   │   │       │   └── VolatilitySurface.java      # Per-instrument vol estimates
│   │   │       │
│   │   │       ├── simulation/                     # Monte Carlo engine — the compute core
│   │   │       │   ├── MonteCarloEngine.java        # Orchestrates all simulation stages
│   │   │       │   │
│   │   │       │   ├── rng/                        # Random number generation
│   │   │       │   │   ├── RngFactory.java         # Creates per-thread SplittableRandom
│   │   │       │   │   └── RngStreamPartitioner.java # Assigns non-overlapping RNG streams
│   │   │       │   │
│   │   │       │   ├── pca/                        # Forward curve PCA model
│   │   │       │   │   ├── PcaModel.java           # Holds eigenvectors + factor vols
│   │   │       │   │   ├── PcaModelCalibrator.java # Fits PCA from historical data
│   │   │       │   │   ├── CurveSimulator.java     # Simulates 60-point curve change
│   │   │       │   │   └── CholeskyDecomposition.java # Correlates random factors
│   │   │       │   │
│   │   │       │   ├── jumpdiffusion/              # Geopolitical shock model
│   │   │       │   │   ├── MertonJumpProcess.java  # Poisson jump sampler
│   │   │       │   │   └── JumpCalibrator.java     # Estimates lambda from history
│   │   │       │   │
│   │   │       │   └── pricing/                    # Scenario P&L computation
│   │   │       │       ├── PortfolioPricer.java    # Loops over positions, sums P&L
│   │   │       │       └── InstrumentPricer.java   # sealed strategy: price per type
│   │   │       │
│   │   │       ├── aggregation/                    # P&L sorting → VaR/CVaR
│   │   │       │   ├── VarAggregator.java          # Reads Arrow buffer, computes quantiles
│   │   │       │   └── ArrowPnlBuffer.java         # Wraps Apache Arrow off-heap vector
│   │   │       │
│   │   │       ├── persistence/                    # TimescaleDB result storage
│   │   │       │   ├── VarResultRepository.java    # Writes VaR results
│   │   │       │   ├── AuditTrailRepository.java   # Stores run inputs + seeds
│   │   │       │   └── SchemaInitializer.java      # Runs schema.sql on startup
│   │   │       │
│   │   │       ├── infrastructure/                 # Wiring layer — no business logic
│   │   │       │   ├── EventBus.java               # LMAX Disruptor facade
│   │   │       │   ├── DisruptorFactory.java       # Builds and configures the ring buffer
│   │   │       │   └── ConnectionPool.java         # JDBC pool for TimescaleDB
│   │   │       │
│   │   │       └── monitoring/                     # SLA watchdog + Micrometer metrics
│   │   │           ├── SlaWatchdog.java            # Background thread watching run progress
│   │   │           ├── ProgressEstimator.java      # Predicts if run will meet deadline
│   │   │           └── MetricsRegistry.java        # Central Micrometer MeterRegistry setup
│   │   │
│   │   └── resources/
│   │       ├── application.properties              # Kafka, DB, SLA config values
│   │       ├── db/
│   │       │   └── schema.sql                      # TimescaleDB table DDL
│   │       └── logback.xml                         # Structured JSON logging config
│   │
│   └── test/
│       └── java/
│           └── com/riskplatform/
│               ├── domain/
│               │   └── InstrumentTypeTest.java         # sealed class pattern matching
│               ├── simulation/
│               │   ├── MonteCarloEngineTest.java        # full sim with known seed
│               │   ├── pca/
│               │   │   ├── CholeskyDecompositionTest.java
│               │   │   └── CurveSimulatorTest.java
│               │   └── jumpdiffusion/
│               │       └── MertonJumpProcessTest.java
│               └── aggregation/
│                   └── VarAggregatorTest.java           # verify quantile correctness
│
├── config/                                 # External service configuration
│   ├── prometheus.yml                      # Scrape targets
│   └── grafana/
│       └── var_dashboard.json             # Pre-built Grafana dashboard
│
├── docker/                                 # Local dev environment
│   ├── docker-compose.yml                  # Kafka + TimescaleDB + Prometheus + Grafana
│   ├── kafka/
│   │   └── kafka.env                       # Broker config (partitions, retention)
│   └── timescaledb/
│       └── init.sql                        # DB init: create DB, enable extension
│
├── .gitignore
├── pom.xml                                 # Maven build — dependencies declared here
└── README.md
```

---

## Package Design Principles

### Why `domain/` has zero dependencies

The `domain` package contains only Java records and enums. It imports nothing from Kafka, Chronicle, Arrow, or any framework.

**Why**: If `Position.java` imports a Chronicle Map class, you cannot test it without Chronicle Map on the classpath. You cannot reuse it in a different context. The domain model should be portable and testable in isolation. This is the **Dependency Inversion** principle applied at the package level.

---

### Why `simulation/` has sub-packages

The simulation package is split into `rng/`, `pca/`, `jumpdiffusion/`, and `pricing/` because each represents a **distinct mathematical concern** with its own calibration inputs and testability requirements.

If everything lived flat in `simulation/`, a change to the jump-diffusion model would force you to recompile the PCA classes. Sub-packages enforce that each concern is independently changeable.

---

### Why `infrastructure/` exists as a separate package

`infrastructure/` contains the wiring code — how Disruptor is configured, how JDBC connections are pooled. It contains **no business logic**. It knows about all other packages but no other package knows about it.

**Why**: This is the entry point for changes caused by external concerns (upgrading Disruptor version, switching connection pool library). Keeping it isolated means a dependency upgrade only touches one package.

---

### Why `store/serialization/` is a sub-package

Chronicle Map requires you to define how Java objects become bytes. This serialization contract is an **implementation detail of the store** — nothing else should know about it. By nesting it inside `store/`, you make it invisible to the simulation engine.

---

### Why tests mirror `main/` structure

Each test class is in the same package path as the class it tests (but under `test/`). This gives the test access to package-private methods without exposing them publicly. `CurveSimulatorTest` lives in `com.riskplatform.simulation.pca` — same package as `CurveSimulator`.

---

## Dependency Flow (must never be violated)

```
infrastructure
    |
    +---> monitoring
    |
    +---> persistence ------> domain
    |
    +---> aggregation ------> domain
    |
    +---> simulation  ------> domain
    |         |
    |         +---> marketdata --> domain
    |
    +---> store       ------> domain
    |
    +---> ingestion   ------> domain
                               ^
                               |
                           (everything reads domain,
                            nothing writes back to it)
```

**The rule**: arrows point inward toward `domain`. No package inside `domain/` may import from `simulation/`, `store/`, or any other package. If you find yourself wanting to add a Kafka import to a domain class — stop. You have a design error.

---

## Key Files Explained

| File | Purpose |
|------|---------|
| `RiskPlatformMain.java` | Wires all subsystems together, starts Disruptor, Kafka consumer, SLA watchdog |
| `MonteCarloEngine.java` | Orchestrates: snapshot positions → simulate curves → price portfolio → write Arrow buffer |
| `ArrowPnlBuffer.java` | The Apache Arrow off-heap vector that holds all 500,000 P&L values |
| `SlaWatchdog.java` | Background thread; if progress at T+4min predicts miss at T+10min, fires alert |
| `AuditTrailRepository.java` | Stores the exact RNG seeds + market data snapshot used in each VaR run |
| `schema.sql` | TimescaleDB DDL: creates `var_results` hypertable partitioned by `run_timestamp` |
| `docker-compose.yml` | Spins up Kafka, TimescaleDB, Prometheus, Grafana for local development |

---

*Updated: Phase 1, Section 2 — after domain model design*
