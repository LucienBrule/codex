Title: VM-PTY cold-path stabilization — admission control, prewarm pool, and readiness gating
Status: Proposed
Date: 2025-11-03
Owners: VM‑PTY lane (Management Codex)

Context
- We validated VM‑PTY at scale with warm and cold measurements.
- Warm round‑trip latency (RTT) remains strong even under concurrency:
  - 5 workers, 25 iters: warm overall p95 ≈ 114 ms
  - 10 workers, 25 iters: warm overall p95 ≈ 118 ms
- Cold path saturates at 10‑up, producing high p95s:
  - cold_open_ms p95 ≈ 98.7 s (clustered near a ~90 s ceiling)
  - cold_first_exec_ms p95 ≈ 75.2 s
- Shapes point to boot‑storm contention (CPU/IO/virtiofsd), plus some runs issuing first exec before the guest shell is fully ready (no prompt gating in some paths).

Problem Statement
Under parallel opens (≥10 workers), VM provisioning and guest bring‑up contend for host resources, leading to long tails for “cold open” and “first exec”. We need systematic controls to cap cold p95 without regressing warm RTT or throughput.

Goals
- Keep warm RTT p95 ≤ 150 ms at 10 parallel workers (currently ~118 ms).
- Reduce cold_open_ms p95 from ~98.7 s to ≤ 45 s at 10‑up.
- Reduce cold_first_exec_ms p95 from ~75 s to ≤ 15 s at 10‑up.
- Ensure stable teardown (no residual domains) and deterministic readiness.

Non‑Goals
- Changing guest feature set beyond boot/readiness essentials.
- Optimizing for macOS/Windows backends (Linux focus only).

Decision (high‑leverage measures)
1) Readiness gating for first exec
   - After nonblocking pty_open, always call pty_read_until(PROMPT/TOKEN) before the first pty_exec.
   - Enforce at both harness/tests and agent paths to avoid race‑dependent tails.

2) Admission control for cold spawns (server‑side)
   - Add a spawn limiter in ptyd: allow at most K concurrent cold boots (initially K=3; configurable).
   - Additional pty_open requests are enqueued; emit events (vm_provision_queued, vm_provision_started) to expose queueing delay.

3) Pre‑warm pool of hot VMs
   - Maintain N prestarted VMs idling at PROMPT (N=3–5; configurable).
   - pty_open attaches to a warm instance (lease token) and immediately becomes usable; a background refill keeps pool size.
   - Implement attach‑first/idempotent open semantics.

4) Resource and IO tuning
   - vCPU pinning/spread and modest memory reservations to avoid boot‑time starvation.
   - Add virtio RNG device to mitigate entropy stalls.
   - Place overlays on fast NVMe (or tmpfs in CI) to reduce boot IO latency.
   - Libvirt/QEMU disk tuning for qcow2: cache=none, io=native, discard=unmap, lazy_refcounts=on.
   - Virtiofs tuning: increase thread pool size and queue depth where supported.

5) Smarter readiness + backoff
   - If PROMPT not seen within T=60 s, destroy/recreate overlay and retry with exponential backoff; emit failure/ retry events.

6) Telemetry and phase timing
   - Emit sequenced events on codex.vm.<vm_id>.events:
     vm_provision_requested → vm_provision_queued → vm_provision_started → vm_provisioned → guest_hello → prompt_seen → first_exec_begin → first_exec_end → vm_teardown.
   - Compute and surface: boot time, shell ready, first exec duration, warm RTT p95/p99.

Rationale
- PROMPT gating removes readiness races that inflate cold_first_exec.
- Admission control prevents boot storms that saturate CPU/IO and virtiofsd.
- Pre‑warm pool converts cold attach p95 into a manageable queueing problem and amortizes boot cost.
- IO/CPU tuning addresses systemic contention and entropy stalls.
- Telemetry provides the phase visibility needed to verify improvements.

Implementation Plan (phased)
- Phase A — Readiness + Harness
  - Update harness/tests to pty_read_until(PROMPT) immediately after nonblocking open.
  - Prefer exec boundary tokens (BEGIN/END with RS framing) over substring matching.

- Phase B — Admission Control (ptyd)
  - Add configurable K for concurrent cold boots; queue beyond K.
  - Emit vm_provision_queued/started events with queue depth.

- Phase C — Pre‑Warm Pool
  - Add N‑sized warm pool with attach‑first semantics and lease tokens.
  - Background refill with staggered starts; pool health metrics.

- Phase D — Resource/IO Tuning
  - Update domain XML template to include RNG, disk/virtiofs settings, and optional vCPU pinning.
  - Document host storage placement for overlays and virtiofsd options.

- Phase E — Telemetry & CI
  - Add phase events and surface per‑phase timings in artifacts.
  - Extend scale tests to report cold vs warm p95s under concurrency.

Acceptance Criteria
- 10 parallel workers, 25 iterations each, on the current host:
  - warm overall p95 ≤ 150 ms (baseline ~118 ms maintained)
  - cold_open_ms p95 ≤ 45 s
  - cold_first_exec_ms p95 ≤ 15 s
  - No residual libvirt domains post‑run; vm_teardown events present for all sessions

Operational Notes
- Prefer group‑based libvirt access (no interactive sudo), with socket ACLs and polkit rule for org.libvirt.unix.manage.
- Keep per‑run logs: server (.codex/codex/logs/ptyd-*.log), harness (.codex/codex/logs/scale*-*.log), artifacts (artifacts/qa/scale*-*.log).

Artifacts (untracked)
- artifacts/adr/vm-pty-cold-start-stabilization/scale5-1762206153-warmcold.log
- artifacts/adr/vm-pty-cold-start-stabilization/scale10-1762207433-warmcold.log
- artifacts/adr/vm-pty-cold-start-stabilization/scale5-1762205514.log

Note: files under artifacts/adr are not tracked in version control; paths are for operator reference.

Open Questions
- Pool sizing policy: static N vs adaptive based on recent demand and boot rate.
- Lease semantics for attach‑first when multiple clients contend for a warm VM.
- Whether to isolate virtiofsd instances per VM for better scaling on some hosts.

Alternatives Considered
- Launching all VMs upfront per worker (static 1:1 mapping). Rejected due to idle cost and reduced elasticity.
- Increasing server‑side timeouts only. Rejected; masks the problem and worsens tail latency.

References (from recent runs)
- 5 workers, 25 iters: warm overall p95 ≈ 114 ms
- 10 workers, 25 iters: warm overall p95 ≈ 118 ms; cold_open_ms p95 ≈ 98.7 s; cold_first_exec_ms p95 ≈ 75.2 s
