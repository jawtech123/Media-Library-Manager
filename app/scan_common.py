from __future__ import annotations
import os
from pathlib import Path
from .settings import Settings


def default_inode_key(stat: os.stat_result) -> str:
    # Cross-platform identity: size-mtime-ctime + (inode/dev if available)
    parts = [
        str(stat.st_size), str(int(stat.st_mtime)), str(int(getattr(stat, "st_ctime", 0))),
        str(getattr(stat, "st_ino", 0)), str(getattr(stat, "st_dev", 0)),
    ]
    return ":".join(parts)


def classify_extension(ext: str, settings: Settings) -> str:
    e = (ext or "").lower()
    if e in set(map(str.lower, settings.media_extensions)):
        return "video"
    if e in set(map(str.lower, settings.image_extensions)):
        return "image"
    if e in set(map(str.lower, settings.subtitle_extensions)):
        return "subtitle"
    if e in set(map(str.lower, settings.xml_extensions)):
        return "xml"
    if e in set(map(str.lower, getattr(settings, 'other_extensions', []))):
        return "other"
    return "unknown"
