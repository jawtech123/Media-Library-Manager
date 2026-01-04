from dataclasses import dataclass, field
from pathlib import Path
from typing import Literal

HashAlgo = Literal["blake3", "xxhash64", "sha256"]

@dataclass
class Settings:
    db_path: Path = Path.home() / ".medialib" / "medialib.db"
    hash_algo: HashAlgo = "xxhash64"
    hash_sample_size: int = 4 * 1024 * 1024  # 4MB sample
    do_full_hash: bool = True  # default on to compute full hashes
    follow_symlinks: bool = False
    use_trash: bool = True  # if False, permanent delete
    max_workers: int = 4
    skip_unchanged: bool = True  # skip hashing/metadata if size/mtime/inode unchanged

    # Media/junk configuration
    media_extensions: list[str] = field(default_factory=lambda: [
        ".mp4", ".mkv", ".avi", ".mov", ".wmv", ".m4v", ".mpg", ".mpeg", ".ts", ".m2ts", ".webm", ".flv"
    ])
    image_extensions: list[str] = field(default_factory=lambda: [
        ".jpg", ".jpeg", ".png", ".webp", ".gif", ".tbn"
    ])
    subtitle_extensions: list[str] = field(default_factory=lambda: [
        ".srt", ".ass", ".ssa", ".vtt", ".sub", ".idx", ".sup"
    ])
    xml_extensions: list[str] = field(default_factory=lambda: [
        ".xml", ".nfo"
    ])
    other_extensions: list[str] = field(default_factory=list)
    junk_patterns: list[str] = field(default_factory=lambda: [
        "*.part", "*.partial", "*.!qb", "*.crdownload", "*.tmp", "*.temp",
        "*.r00", "*.r01", "*.r02", "*.rar", "*.zip", "*.7z", "*.par2"
    ])
    junk_exclude_extensions: list[str] = field(default_factory=list)

    naming_template: str = "{show} - S{season:02d}E{episode:02d}"
    ingest_host: str = "0.0.0.0"
    ingest_port: int = 8765
    ingest_token: str = ""  # empty disables auth for local-only use
    agent_batch_size: int = 500
    agent_max_workers: int = 4
    agent_gzip: bool = False
    agent_adaptive: bool = True
    agent_offpeak_start: int = 1  # 1am
    agent_offpeak_end: int = 6    # 6am
    walk_mode: Literal["batched","dfs","bfs"] = "batched"


def ensure_app_dirs(settings: Settings) -> None:
    settings.db_path.parent.mkdir(parents=True, exist_ok=True)
