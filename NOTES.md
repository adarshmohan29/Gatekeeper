# Notes

What broke, what was measured, what surprised me. Newest first.

## Phase 0: Setup (2026-11-20)
- Local Python is 3.11 (no 3.12 installed), so the project targets `>=3.11`.
- Docker is not installed yet; it's needed before Phase 1.
- Flow event schema (`shared/events.py`) is per-packet for now. Revisit in Phase 1: per-packet vs per-flow aggregates.
