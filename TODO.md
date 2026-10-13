# TODO

Open work for rrx-core. Benchmarks run from
[ren-bench](../../../ren-bench/wtree/_repo_/README.md); performance defects
are listed here.

- [ ] Async parity: the async workload runs at 1.16 times the fastest
  market library (median paired ratio, 30 ABBA pairs, A/A
  control 1.00), and steps at 1.01, against a target of parity (the market
  table is in ren-bench's reactivity guide, Current results). By phase (rrx-core over that library, 10 paired
  processes): building the 1,000 tasks 1.05, a round of completions 1.20,
  a write that relaunches them 1.16, the ignored late completions 1.85,
  reading the 1,000 values 1.80, disposal 0.74. The gap has no hot spot:
  it is the contract's work per launch and completion (the pending and
  ready publications, the completion's checks for a stopped owner and a
  changed input, the result record and the flush after each completion,
  the readiness read), each a few nanoseconds, and no task function holds
  more than 8% of a profile. Neutral in a paired run: dropping the relaunch
  check or the status-only reader walk, a smaller launch with its rare paths
  in helpers, the record's hot fields first, and `instanceof` against a
  module-local class. A change must cut that work without a branch in
  every plain memo computation, and keep readiness snapshots, pending
  reuse, source and launch contexts, cancellation, stale completions,
  disposal and transitions; it passes the suites and a paired async/steps
  comparison before a full market run.
