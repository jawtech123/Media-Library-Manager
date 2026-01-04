# Media Library Manager

Media Library Manager is a local/remote media ingest and organization tool designed to help you:

- Discover and index media files across one or more roots (local and remote)
- Compute fast and full hashes to identify duplicates and near-duplicates
- Extract video metadata (via ffprobe) for richer classification and insights
- Detect and surface unneeded junk files (e.g., partial downloads, archives)
- Organize and rename media according to consistent naming templates (organizer)

The system consists of a host application (ingest server + GUI) and a lightweight agent that can run on remote machines to scan mounted disks or NAS shares and send batches of file records to the host.

## Key Features

- Two-pass ingest for responsiveness and completeness:
  - Pass 1 (fast path): classify + hash; no ffprobe to keep UI responsive
  - Pass 2 (enrichment): ffprobe only for video; no recompute of hashes
- Deduplication support via consistent hashing (sample + full) and inode-based identity keys
- Junk classification using configurable glob patterns and extension exclusions
- Resume-safe scanning: the agent stores a per-root, per-phase cursor to continue where it left off
- Offline-tolerant agent: batches are queued locally and resent when the host becomes available
- Configurable batch size, worker concurrency, and hashing parameters from the host
- Organizer to help rename and arrange files into target folder layouts (work in progress)

## Components Overview

- Host (ingest server):
  - Endpoint `/ingest/batch` accepts JSON payloads `{ "batch_id": string, "files": FileRecord[] }`
  - Maintains the database of ingested files and metadata
  - Provides configuration to agents via `/ingest/config`
  - GUI for reviewing junk, duplicates, and unknown items; organizer UI for WIP renaming workflows

- Agent (standalone or alongside host):
  - Scans configured remote roots, classifies files, computes hashes, and runs ffprobe (videos)
  - Sends batches to the host using plain JSON POST (no tokens/headers required)
  - Exposes a small HTTP control/diagnostics server on the agent side (`/agent/*`)
  - Uses an embedded SQLite cache to avoid re-probing/hashing and to support resume/offline behavior

## Data Flow

1) The host exposes `/ingest/config` with all scanning settings and remote roots.
2) The agent fetches config and scans in two passes:
   - Pass 1: compute and send hashes, skip ffprobe (all kinds)
   - Pass 2: ffprobe videos only and send metadata
3) Batches are posted to `/ingest/batch`. If the host is offline, batches are enqueued locally and retried later.
4) The host upserts items per-file, tolerating partial failures.

## Duplicate and Junk Detection

- Hashing:
  - Sample hash: uses a small portion(s) of the file to create a quick signature
  - Full hash: performed during off-peak hours (configurable) to minimize disk impact
- Duplicate detection (in the host): based on sample/full hash matches and inode identity
- Junk detection:
  - Configurable glob patterns (e.g., `*.part`, `*.r00`, `*.par2`, etc.)
  - Exclusion list for extensions that should never be considered junk

## Organizer (Work in Progress)

- Applies naming templates (e.g., `{show} - S{season:02d}E{episode:02d}`) to files
- Helps move/rename files into structured folder layouts
- Integrates with the scanned metadata and classification
- Currently under active development; behavior and UI subject to change

## Running the Agent

- Standalone: `python agent.py <host-ip-or-url>`
- The agent starts a small local HTTP server for diagnostics and control at port 8877 by default
- Use `--clear-cache` to delete the local agent cache database on startup

## Status and Diagnostics

- Agent endpoints (on the agent machine, default port 8877):
  - `GET /agent/ping` — health probe for the agent process
  - `GET /agent/stats` — current scan statistics and progress
  - `GET /agent/ls?path=<dir>` — browse filesystem to help select remote roots
  - `POST /agent/scan_now` — trigger a scan cycle immediately
  - `POST /agent/clear_cache` — delete the embedded cache DB file
  - `GET /agent/cache_info` — summarize cache DB and table row counts
  - `POST /agent/compact_cache` — VACUUM the cache database

## Design Principles

- JSON-only HTTP: all exchanges between agent and host use standard JSON POST
- No tokens or secrets in the agent or server; local-only deployments by default
- Centralized scan/classification logic to keep consistency between local and remote contexts
- Efficient, resilient scanning that can stop/restart without losing progress

## Deep Dive Architecture

The system is split into a host app and a remote agent, communicating via plain JSON over HTTP.

- Host (ingest server + GUI)
  - Accepts batches via `POST /ingest/batch` and upserts them into a local SQLite/DB (see `app/db.py`).
  - Provides `GET /ingest/config` which agents poll for settings (e.g., roots, batching, hashing, throttling).
  - GUI surfaces:
    - Junk tab for quick triage of unwanted files (e.g., partial downloads, archives).
    - Duplicates tab for inspecting duplicate clusters detected by hashes.
    - Unknown tab for items not recognized by classification.
    - Organizer tab (WIP) to preview/commit renames and moves.
  - The local scanner (if enabled) shares classification logic (`app/scan_common.py`) with the agent.

- Remote Agent
  - Standalone `agent.py` with embedded SQLite cache for:
    - `agent_index`: inode identity and last probe/hash results.
    - `outbox`: queued batches when offline.
    - `scan_progress`: per-root, per-phase cursors to resume.
  - Two-pass scanning:
    - Pass 1 (fast): classify + compute sample (and optionally full) hashes; do not run ffprobe.
    - Pass 2 (metadata): ffprobe for videos only; do not recompute hashes.
  - Posting:
    - JSON body `{ "batch_id": string, "files": [...] }` to `/ingest/batch`.
    - Optional gzip (controlled by host config); no tokens or custom headers.
  - Offline/Resume behavior:
    - If the host is unreachable or batch post fails, enqueue payload in `outbox` and continue scanning.
    - Drain `outbox` on startup and after any successful post.
    - Resume from `scan_progress` cursors after restarts.

## Operational Workflows

- Initial setup
  1. Start host: `python main.py` (defaults to `0.0.0.0:8765`).
  2. Configure `remote_roots` via settings or GUI.
  3. Start agent(s): `python agent.py <host>` on each remote machine.

- Day-to-day ingest
  - Agents perform two-pass scans, continuously flushing batches by size and time to keep the UI responsive.
  - Hashes are reused if inode identity and hashing parameters match; ffprobe is skipped if already probed for the inode.
  - Dupes/junk become visible in the GUI as batches land.

- Offline periods
  - Agents continue scanning and queue batches in `outbox`.
  - Once the host is reachable, outbox drains automatically.

- Organizer (WIP)
  - Use the naming template in settings to preview changes.
  - Commit operations will rename/move to target structure (additional safety checks planned).

## Roadmap Ideas

- Enhanced duplicate detection heuristics across different container formats
- More robust organizer workflows, including conflict resolution and preview modes
- Automated clean-up suggestions for junk and unwanted items
- Configurable backoff and retry strategies for outbox draining
- Richer telemetry and metrics for large-scale scans
