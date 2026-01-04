# Media Library Manager

Media Library Manager is a local/remote media ingest and organization tool. It scans one or more roots, computes fast/full hashes for duplicate detection, runs ffprobe on videos for rich metadata, flags junk/unwanted files, and provides an organizer (work in progress) to rename and arrange files.

- Two-pass ingest: fast hashes first, then video metadata
- Duplicate detection with sample + full hashing
- Junk detection via patterns and exclusions
- Resume-safe scans and offline queueing (agent outbox)
- Embedded SQLite cache in the agent; single-file deployment
- GUI for review (junk, duplicates, unknowns) and organizer flows (WIP)

## Architecture

- Host application
  - Ingest server exposes:
    - `POST /ingest/batch`: JSON-only batches `{ "batch_id": string, "files": FileRecord[] }`
    - `GET /ingest/config`: agent configuration
  - Stores items in a local database
  - GUI to review scanned items and run organizer workflows
- Remote Agent
  - Scans remote roots and posts batches to the host
  - Two-pass scanning: hashes (no ffprobe), then ffprobe for video
  - Embedded cache for probe/hash reuse, outbox for offline queueing, and scan progress cursors
  - Local HTTP endpoints on port 8877 for diagnostics and control

## Quick Start

Prerequisites:
- Python 3.10+
- ffprobe in PATH on agent machines (for video metadata)

Install dependencies:
- Host: `pip install -r requirements.txt`
- Agent: `pip install -r agent_requirements.txt` (or reuse `requirements.txt` if shared)

Run the host:
- `python main.py`
- Default host URL for agents: `http://localhost:8765`

Configure remote roots:
- The host provides config through `/ingest/config`.
- Adjust app/settings.py or GUI settings to set `remote_roots` and other agent parameters.

Run the agent (remote machine):
- Copy `agent.py` (single file)
- `python agent.py <host-ip-or-url>`
- Optional: `python agent.py <host> --clear-cache` to reset the embedded cache

## Agent specifics

- Single-file, embedded SQLite cache at `~/.medialib/agent_cache.db` (auto-created)
- Two-pass scanning:
  - Pass 1 (fast path): classify + hashes; no ffprobe
  - Pass 2 (enrichment): ffprobe videos only; no recompute of hashes
- Offline handling: enqueue failed batches in outbox and drain on startup and after any successful post
- Resume scanning: cursor per root and phase in `scan_progress`
- Batch flush: by size (`agent_batch_size`) and by time (~2s)
- Endpoints (port 8877): `/agent/ping`, `/agent/stats`, `/agent/ls`, `/agent/scan_now`, `/agent/clear_cache`, `/agent/cache_info`, `/agent/compact_cache`

## Data contracts

- `/ingest/batch` accepts: `{ "batch_id": string, "files": FileRecord[] }`
- FileRecord fields:
  - Common: `kind`, `path`, `size`, `mtime`, `ctime`, `inode_key`, `ext`
  - Junk: `reason`
  - Hashes: `{ algo, sample_size, sample_hash, full_hash }` (optional)
  - Video: `{ duration, container, video_codec, audio_codecs, width, height, bitrate, streams_json }` (optional)

## Duplicate and junk detection

- Hashing: sample hash for speed; full hash during off-peak hours
- Duplicates: by sample/full hash and inode identity
- Junk: patterns like `*.part`, `*.par2`, `.rNN`, archives; exclusion lists prevent false positives

## Organizer (WIP)

- Renaming using templates like `{show} - S{season:02d}E{episode:02d}`
- Folder organization workflows
- Integrates with scanned metadata; UI evolving

## Configuration

See `app/settings.py` for defaults and descriptions:
- `remote_roots`, `hash_algo`, `hash_sample_size`, `do_full_hash`
- `agent_batch_size`, `agent_max_workers`, `agent_gzip`, `agent_adaptive`
- `agent_offpeak_start`, `agent_offpeak_end`
- `follow_symlinks`, `junk_patterns`, `junk_exclude_extensions`
- `media_extensions`, `image_extensions`, `subtitle_extensions`, `xml_extensions`

## Development

- Create a venv: `python -m venv .venv && source .venv/bin/activate` (or `.venv\\Scripts\\activate` on Windows)
- Install: `pip install -r requirements.txt`
- Run host: `python main.py`
- Run agent: `python agent.py http://localhost:8765`
- Keep scanning logic centralized in `app/scan_common.py`
- Optional tooling: Black/Ruff/mypy (configs can be added)

## Troubleshooting

- Host offline: the agent queues batches (see `/agent/stats` and `/agent/cache_info`)
- No video metadata: ensure ffprobe is installed on the agent host
- Cache issues: use `/agent/clear_cache` or `--clear-cache`

## License

Apache-2.0
