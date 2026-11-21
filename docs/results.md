# Detector comparison

Generated 2026-11-20 by `make evaluate` from `data/dataset.parquet` (`make dataset`).

- **Data:** 13 training sessions (1514 vectors), 15 test sessions (1809 vectors), split by session. Never in training: flash, ramp, udp (UDP is an unseen attack type; flash crowds and slow ramps are the hard cases).
- **Same input for all:** each detector scores the same recorded test vectors; flagged = score >= 0.5.
- **Delays** are in 1 s windows from an attacker's first vector; the live pipeline adds about 1 s of capture and Kafka latency (Gates 2-3).

| Metric | threshold | xgboost | autoencoder |
|---|---|---|---|
| False-positive rate, normal traffic | 0.0% | 0.0% | 0.1% |
| False-positive rate, flash crowd | 73.6% | 0.0% | 100.0% |
| Detection rate, seen attacks (SYN/HTTP/mixed) | 100.0% | 100.0% | 100.0% |
| Detection rate, unseen attack type (UDP) | 100.0% | 0.0% | 100.0% |
| Detection rate, slow ramp | 100.0% | 100.0% | 100.0% |
| Median detection delay, seen attacks (s) | 0.0 | 0.0 | 0.0 |
| Median detection delay, UDP (s) | 0.0 | never detected | 0.0 |
| Median detection delay, slow ramp (s) | 0.0 | 0.0 | 0.0 |
| Median time to mitigation, all attacks (3 windows in a row, s) | 2.0 | 2.0 | 2.0 |
| Throughput (windows/s, 1 core) | 1,951,435 | 159,942 | 110,123 |
| CPU per window (µs) | 0.5 | 6.2 | 9.1 |
| Memory added by loading and running the detector (MB) | 2.6 | 10.3 | 10.7 |
| Model size on disk (KB) | 0.1 | 17.4 | 15.9 |
