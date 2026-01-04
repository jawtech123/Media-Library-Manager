from __future__ import annotations
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Optional

# Basic patterns similar to Sonarr-like extraction
PATTERNS = [
    re.compile(r"(?P<show>.+?)[. _-]*[Ss](?P<season>\d{1,2})[Eex](?P<episode>\d{2})(?:[Eex](?P<episode2>\d{2}))?(?:[. _-]+(?P<title>.+))?"),
    re.compile(r"(?P<show>.+?)[. _-]*(?P<season>\d{1,2})x(?P<episode>\d{2})(?:x(?P<episode2>\d{2}))?(?:[. _-]+(?P<title>.+))?"),
    re.compile(r"(?P<show>.+?)[. _-]*Season[. _-]*(?P<season>\d{1,2})[. _-]*Episode[. _-]*(?P<episode>\d{1,3})(?:[. _-]+(?P<title>.+))?", re.IGNORECASE),
]

@dataclass
class Parsed:
    show: str
    season: int
    episode: int
    episode2: Optional[int]
    title: Optional[str] = None


def parse_filename(name: str) -> Optional[Parsed]:
    base = Path(name).stem
    for pat in PATTERNS:
        m = pat.search(base)
        if m:
            show = re.sub(r"[._]+", " ", m.group("show")).strip(" -_")
            season = int(m.group("season"))
            episode = int(m.group("episode"))
            episode2 = int(m.group("episode2")) if m.groupdict().get("episode2") else None
            return Parsed(show=show, season=season, episode=episode, episode2=episode2)
    return None


def parse_from_path(p: Path) -> Optional[Parsed]:
    # Try filename first
    parsed = parse_filename(p.name)
    if parsed:
        return parsed
    # Try to infer from folders like Show/Season 01 or Show/S01
    parts = [part for part in p.parts]
    # Search for 'Season xx' or 'Sxx' in parents
    season = None
    show = None
    for i in range(len(parts) - 2, -1, -1):
        seg = parts[i]
        m1 = re.search(r"[Ss](\d{1,2})", seg)
        m2 = re.search(r"Season\s*(\d{1,2})", seg, re.IGNORECASE)
        if m1 or m2:
            season = int((m1 or m2).group(1))
            # previous folder as show name
            if i - 1 >= 0:
                show = re.sub(r"[._]+", " ", parts[i - 1]).strip(" -_")
            break
    if show and season is not None:
        # Try to parse episode from filename 1-3 digits
        m = re.search(r"\b(\d{1,3})\b", p.stem)
        if m:
            ep = int(m.group(1))
            return Parsed(show=show, season=season, episode=ep, episode2=None)
    return None


def propose_path(src: Path, parsed: Parsed, template: str | None = None) -> Path:
    season_dir = src.parent.parent / parsed.show / f"Season {parsed.season:02d}"
    season_dir.mkdir(parents=True, exist_ok=True)
    if template:
        try:
            base = template.format(show=parsed.show, season=parsed.season, episode=parsed.episode)
        except Exception:
            base = f"{parsed.show} - S{parsed.season:02d}E{parsed.episode:02d}"
    else:
        base = f"{parsed.show} - S{parsed.season:02d}E{parsed.episode:02d}"
    if parsed.episode2:
        base += f"E{parsed.episode2:02d}"
    new_name = f"{base}{src.suffix}"
    return season_dir / new_name
