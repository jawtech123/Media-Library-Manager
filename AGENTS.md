# Agent and Ingestion Notes

- Do not use authentication tokens or secrets in the remote agent or ingestion server. Both components must operate without bearer tokens or custom headers.
- All data exchanges must be JSON via standard HTTP POST using a JSON body (no URL params for payloads).
- Remote agent should perform ffprobe metadata extraction for video files to align with local scanner behavior.
- Server accepts metadata provided by the agent and should not perform ffprobe by itself. The local scanner writes metadata directly to the DB; the remote agent includes metadata in the batch payload.
- Keep scanning/classification logic centralized in `app/scan_common.py` to avoid duplication.

## Remote Agent Two-Pass Ingest Flow

The agent performs ingestion in two passes for responsiveness and completeness:

- Pass 1 (fast path):
  - Scans configured remote roots
  - Classifies files using shared helpers in `app/scan_common.py`
  - Computes hashes (sample and optionally full)
  - Does NOT run ffprobe
  - Posts batches of file records quickly so the server/UI updates immediately

- Pass 2 (metadata enrichment):
  - Scans the same roots again
  - Restricts processing to `video` kinds only
  - Runs `ffprobe` for each video and adds simplified metadata
  - Does NOT recompute hashes
  - Posts only metadata-enriched file records

Behavioral notes:
- Both passes post JSON payloads to `/ingest/batch` with the shape `{ "files": [ ... ] }`.
- The server trusts agent-provided metadata and does not run ffprobe itself.
- The agent does not use authentication tokens or custom headers.
- The agent exposes `/agent/stats` with `active`, `uploaded`, `batches`, `errors`, and timing to aid monitoring.
- Batch flush occurs by size and by time (to avoid long quiet periods under slow ffprobe/hashing).

Tuning:
- `agent_batch_size` controls batch size (recommend 100–500 depending on network/latency).
- `agent_max_workers` controls agent threading; increase cautiously (ffprobe can be CPU/disk heavy).
- Hashing behavior is controlled by `hash_algo`, `hash_sample_size`, and `do_full_hash`.
- `follow_symlinks`, `junk_patterns`, and `junk_exclude_extensions` control traversal/classification.

Error handling:
- Failed futures increment `errors` and are skipped; batches continue.
- Partial failures in a batch do not stop ingestion; the server upserts per-item.
- The agent logs to `~/.medialib/agent.log` and console for diagnostics.

## Agent Local Cache

- Location: `~/.medialib/agent_cache.db` (SQLite)
- Purpose: remember per-path inode identity and last known probe/hash results so restarts don’t repeat work.
- Keys:
  - `path`, `inode_key`, `size`, `mtime`, `ctime`
  - `probed` flag (whether ffprobe was completed for current inode)
  - `hashed` flag and hash details: `hash_algo`, `hash_sample_size`, `sample_hash`, `full_hash`, `last_hashed_at`
- Freshness: cache entries are considered valid only if the current file’s `inode_key` matches the cached one and hash parameters match.
- Skipping rules:
  - ffprobe is skipped if `probed=1` and inode matches.
  - Hashes are reused if `hashed=1`, inode matches, and `hash_algo` + `hash_sample_size` match.

Clearing the cache:
- CLI: run the agent with `--clear-cache` to remove the local cache on startup.
- HTTP: POST to the agent endpoint `/agent/clear_cache` (returns `{ ok: true, cleared: true|false }`).

## Agent Specifics and Behavior

- Single-file operation: The agent embeds its own SQLite cache. Deploying `agent.py` alone is sufficient; it will auto-create `~/.medialib/agent_cache.db` on first run.
- Two-pass scanning:
  - Pass 1 (hashes): all file kinds; computes sample hash and (optionally) full hash during off-peak hours; does not run ffprobe.
  - Pass 2 (ffprobe): only `video` kind; runs ffprobe to extract simplified metadata; does not recompute hashes.
- Posting: Both passes post to `/ingest/batch` with JSON body `{ "batch_id": string, "files": [...] }`. Gzip can be enabled via config (`agent_gzip`).
- Offline handling:
  - Failed batch posts are enqueued into a local `outbox` table in the cache database and retried later.
  - On startup and after any successful post, the agent drains the outbox until empty.
  - The agent continues running if the host health check fails initially, allowing offline queueing.
- Resume scanning:
  - Per-root, per-phase cursor is stored in the `scan_progress` table (`last_path`).
  - On restart, each root resumes from its last cursor for the phase.
- Cache reuse policy:
  - Hashes are reused only if inode identity and hashing parameters (`hash_algo`, `hash_sample_size`) match.
  - ffprobe metadata is reused if inode identity matches and `probed=1`.
- Adaptive and throttling:
  - Adaptive worker permits adjust concurrency in-flight to balance throughput and backlog.
  - Full hashes are limited to off-peak window (`agent_offpeak_start`..`agent_offpeak_end`).
- Batch flushing:
  - Flushes by size (`agent_batch_size`) and by time (~2s) to avoid long quiet periods.

### Agent HTTP Endpoints (on the agent machine, default port 8877)
- `GET /agent/ping`: quick liveness probe for the agent process
- `GET /agent/stats`: current scan stats: `active`, `uploaded`, `batches`, `errors`, `rate_files_per_s`, totals, phase and counters
- `GET /agent/ls?path=/some/dir`: list directories/files to aid root selection
- `POST /agent/scan_now`: triggers a two-pass scan immediately using current config from host
- `POST /agent/clear_cache`: deletes `~/.medialib/agent_cache.db` and returns `{ ok, cleared }`
- `GET /agent/cache_info`: returns `{ db_path, exists, size_bytes, rows, last, ts }` summary
- `POST /agent/compact_cache`: runs `VACUUM` on the cache DB

### Agent Cache Schema (SQLite)
- `agent_index(path PRIMARY KEY, inode_key, size, mtime, ctime, probed, hashed, hash_algo, hash_sample_size, sample_hash, full_hash, last_seen, last_hashed_at)`
- `outbox(id PRIMARY KEY, batch_id, payload_json, created_at)`
- `scan_progress(root, phase, last_path, updated_at, PRIMARY KEY(root, phase))`

### Configuration Provided by Host
- `remote_roots`: list of directories to scan on the agent
- `hash_algo` (blake3|xxhash64|sha256), `hash_sample_size`, `do_full_hash`
- `agent_batch_size`, `agent_max_workers`, `agent_gzip`, `agent_adaptive`
- `agent_offpeak_start`, `agent_offpeak_end`
- `follow_symlinks`, `junk_patterns`, `junk_exclude_extensions`, media extension lists

### File Record Shape
Each file in a batch may include:
- Common: `kind`, `path`, `size`, `mtime`, `ctime`, `inode_key`, `ext`
- For junk: `reason`
- Hashes (when present): `{ algo, sample_size, sample_hash, full_hash }`
- Video metadata (when present): `{ duration, container, video_codec, audio_codecs, width, height, bitrate, streams_json }`

## Standalone Agent Deployment

You can run the agent on a remote machine by copying files:

Minimum required:
- `agent.py` (includes an embedded cache implementation; no extra files required)

Optional:
- `agent_cache.py` (place alongside `agent.py`) — if present, the agent will use it instead of the embedded cache.

Notes:
- The agent attempts imports in this order: `app.agent_cache` then local `agent_cache`. If neither is present, it falls back to no-cache mode and still runs.
- JSON-only communication to the host at `/ingest/batch`; no tokens or custom headers.
- Start with: `python agent.py <host-ip-or-url>`
- Clear local cache on next start: `python agent.py <host> --clear-cache`
