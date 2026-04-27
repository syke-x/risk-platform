# Crude Oil VaR Risk Platform — Session Progress Tracker

> **Project**: Production-grade Monte Carlo VaR Engine for a Crude Oil Company
> **Stack**: Java 21 · LMAX Disruptor · Chronicle Map · Apache Arrow · Kafka · TimescaleDB · Micrometer/Prometheus
> **Goal**: 500,000 simulations · 100 instruments · <10 seconds · Full audit trail

---

## Overall Progress

| Phase | Title | Status |
|-------|-------|--------|
| Phase 1 | System Architecture & Data Model Design | 🟡 In Progress |
| Phase 2 | High-Performance Random Number Generation | ⬜ Not Started |
| Phase 3 | Crude Oil Forward Curve PCA Model | ⬜ Not Started |
| Phase 4 | *(Content TBD)* | ⬜ Not Started |
| Phase 5 | Real-Time Position Ingestion Pipeline | ⬜ Not Started |
| Phase 6 | Audit Trail, Backtesting & Production Hardening | ⬜ Not Started |

---

## Phase 1 — System Architecture & Data Model Design

**Status**: 🟡 In Progress — Section 1 complete, awaiting verification answer

### Topics Covered

#### Section 1 — Full Component Diagram ✅
- Drew the full ASCII component diagram of all major subsystems
- Identified 8 major subsystems and the protocol at each boundary:
  - **Kafka Consumer** ← Kafka topic (positions.updates)
  - **Position Store** ← Chronicle Map off-heap writes via Disruptor
  - **Market Data Service** ← HTTP/FIX from external provider
  - **Monte Carlo Risk Engine** ← reads Position Store + Market Data
  - **VaR Aggregator** ← Apache Arrow columnar vectors from Risk Engine
  - **TimescaleDB** ← JDBC from VaR Aggregator
  - **Monitoring Layer** ← Micrometer in-process events from all components
  - **Prometheus/Grafana** ← scrapes Monitoring Layer

- **Subsystem boundary rationale explained for each**:
  - Kafka Consumer vs Position Store → different rates of change; slow write blocks consumer causing Kafka lag
  - Position Store vs Risk Engine → mutable state vs stateless computation; Chronicle Map avoids GC pressure
  - Risk Engine vs VaR Aggregator → compute-bound (float arithmetic) vs sort-bound (memory bandwidth); enables single-pass sort on Arrow buffer
  - Monitoring Layer separate → must survive if Risk Engine hangs; silence is the worst failure mode

- **Key design decisions logged**:
  - Chronicle Map chosen over heap-managed Java objects → avoids GC pressure during computation
  - Apache Arrow as Risk Engine → Aggregator protocol → zero-copy columnar, no heap allocation
  - LMAX Disruptor as intra-process event bus → lock-free, cache-friendly ring buffer

#### Section 2 — Domain Model Design ⬜
- `Position` entity fields
- `Instrument` entity (Brent futures vs Basrah Light physical cargo)
- `ForwardCurve` entity (60 monthly price points)
- `SimulationResult` entity (single Monte Carlo trial output)

#### Section 3 — ForwardCurve Memory Layout Tradeoff ⬜
- `double[]` vs `Map<Tenor, Double>` vs Apache Arrow vector
- Memory layout and cache performance analysis

#### Section 4 — VaR Calculation Pipeline Stages ⬜
- Stage-by-stage: input → output for each stage

### Open Verification Questions
- ⏳ **Pending answer**: *"Why is it important that the Risk Engine writes its 500,000 P&L values into a columnar format rather than a `List<Double>` or a `double[]`? Think about what happens to a `List<Double>` in memory and what columnar means for layout."*

---

## Phase 2 — High-Performance Random Number Generation

**Status**: ⬜ Not Started

### Topics to Cover
1. Why `java.util.Random` is unsuitable (thread contention, period length, statistical quality)
2. `SplittableRandom` and the "split" operation for parallel Monte Carlo
3. Mersenne Twister vs PCG vs Xoshiro256 — which to use and why
4. Cholesky decomposition from first principles (correlating 60 curve factors per simulation)
5. Memory layout: `double[500000][60]` vs `double[60][500000]` vs Arrow table — cache implications
6. Partitioning 500,000 simulations across Java 21 virtual threads with non-overlapping RNG streams

---

## Phase 3 — Crude Oil Forward Curve PCA Model

**Status**: ⬜ Not Started

### Topics to Cover
1. Why we can't simulate 60 independent monthly prices; empirical structure of crude curve movements
2. PCA from first principles (eigenvectors, explained variance, how many components to keep)
3. Simulation loop pseudocode (sample 3 standard normals → scale → reconstruct 60-point curve)
4. Jump-diffusion for geopolitical shocks (Merton model, Poisson process, lambda calibration)
5. Basis risk model (Basrah Light vs Brent spread, distribution, correlation with PCA factors)
6. `CurveSimulator` class interface design

---

## Phase 4 — *(Content TBD)*

**Status**: ⬜ Not Started

> Phase 4 file is currently empty. Content will be added when the phase prompt is provided.

---

## Phase 5 — Real-Time Position Ingestion Pipeline

**Status**: ⬜ Not Started

### Topics to Cover
1. Kafka consumer design — partition key strategy, exactly-once semantics, out-of-order updates
2. Chronicle Map for off-heap position storage — off-heap memory, serialization format, failure modes
3. Incremental VaR recalculation — delta-based approach, position-level VaR sensitivity, staleness threshold
4. Fast path / slow path architecture — parametric (milliseconds) vs full Monte Carlo (every 15 min)
5. SLA monitoring — progress estimator, Micrometer metrics, alert thresholds

---

## Phase 6 — Audit Trail, Backtesting & Production Hardening

**Status**: ⬜ Not Started

### Topics to Cover
1. Immutable audit trail design — VaR run record schema in TimescaleDB, storing parallel RNG seeds, append-only tables
2. Backtesting framework — Kupiec POF test, Basel traffic light system, backtesting pipeline over 3-year history
3. Model validation — detecting stale PCA model, correlation matrix stability check
4. Production failure modes & runbooks:
   - Market data feed 20 minutes late
   - Kafka consumer 10,000 messages behind
   - One of 8 simulation worker threads dies mid-run
5. Final architecture review — full system diagram, single points of failure, production additions

---

## Key Design Decisions Log

| Decision | Chosen | Rejected | Reason |
|----------|--------|----------|--------|
| Intra-process event bus | LMAX Disruptor (ring buffer) | BlockingQueue / synchronized | Lock-free, cache-line friendly, predictable latency |
| Position storage | Chronicle Map (off-heap) | Java HashMap (heap) | Eliminates GC pressure during VaR computation |
| Simulation output format | Apache Arrow (columnar) | `List<Double>` / `double[]` (heap) | Zero-copy, cache-friendly, no GC allocation |
| Position ingestion | Kafka | Direct TCP / REST | Persistent, replayable, ordered per partition |
| VaR result persistence | TimescaleDB | Plain PostgreSQL | Native time-series compression and partitioning |
| Framework | Lean Java (no Spring Boot) | Spring Boot | Understand every layer; eliminate hidden latency |

---

## Concepts Introduced

| Concept | Phase | Summary |
|---------|-------|---------|
| Subsystem boundary principle | 1 | Separate when: different rate of change, failure mode, or scaling dimension |
| Off-heap memory (Chronicle Map) | 1 | Memory outside JVM heap; not subject to GC; requires explicit serialization |
| LMAX Disruptor | 1 | Lock-free ring buffer for intra-process messaging; avoids lock contention |
| Apache Arrow columnar format | 1 | Values stored column-by-column; cache-efficient for column-wise operations |
| VaR / CVaR | 1 | 95%/99% loss quantile; CVaR = mean of losses beyond the VaR threshold |

---

*Last updated: Phase 1, Section 1 complete*
