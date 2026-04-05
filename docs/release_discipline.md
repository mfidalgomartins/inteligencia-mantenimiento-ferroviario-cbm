# Release Discipline

## Objective
Prevent silent quality erosion and false confidence by enforcing explicit release states and blockers.

## Release States
- `technically_valid`: no critical failures and no publish blockers.
- `analytically_acceptable`: technically valid plus no high-severity failures.
- `decision-support only`: technically valid but not analytically acceptable.
- `screening-grade only`: analytically acceptable but below committee-grade thresholds.
- `not committee-grade`: any state below committee-grade.
- `publish-blocked`: at least one failed publish blocker.

## Primary Release State
Computed in `outputs/reports/release_readiness.csv` as:
1. publish-blocked
2. committee-grade
3. decision-support only
4. screening-grade only
5. analytically acceptable
6. technically valid
7. not committee-grade

## Blocking Rules
- Missing required datasets/artifacts.
- Critical semantic inconsistencies.
- Governance contract non-compliance (critical/high).
- High-risk decision logic failures.
- Material narrative/output mismatches.

## Required Artifacts Before Publish
1. `outputs/reports/publish_blockers.csv` (must be empty).
2. `outputs/reports/governance_contract_blockers.csv` (must be empty).
3. `outputs/reports/release_readiness.csv` (state must be explicit).
4. `outputs/reports/validation_report.md`.
5. `outputs/reports/release_artifact_manifest.csv` (hash manifest for critical artifacts).
6. `outputs/reports/release_hardening_checks.csv` (final release gates).
7. `outputs/reports/release_hardening_report.md`.

## Final Release Hardening Layer
- Implemented in `src/release_hardening.py`.
- Adds deterministic artifact manifest (`sha256`, size, last modified UTC).
- Verifies:
  - single official dashboard artifact,
  - dashboard signature consistency (header stamp vs payload meta),
  - strict offline mode (no external CDN refs),
  - executive decision alignment with SSOT narrative metrics,
  - readiness state present and not publish-blocked,
  - validation outputs generated.

## Publish Gate
Release should be considered blocked if:
- `release_hardening_checks.csv` has any failed `critica` check.
- `publish_blockers.csv` is non-empty.
- `governance_contract_blockers.csv` is non-empty.

## Caveat
A `committee-grade` synthetic run is still not equivalent to real-world deployment readiness without calibration on historical operational data.
