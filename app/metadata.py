from __future__ import annotations
import json
import shutil
import subprocess
from pathlib import Path


def has_ffprobe() -> bool:
    return shutil.which("ffprobe") is not None


def probe_ffprobe(path: Path) -> dict:
    cmd = [
        "ffprobe",
        "-v",
        "error",
        "-show_format",
        "-show_streams",
        "-print_format",
        "json",
        str(path),
    ]
    try:
        out = subprocess.check_output(cmd, stderr=subprocess.STDOUT)
        data = json.loads(out.decode("utf-8", errors="ignore"))
    except Exception:
        return {}
    # extract simplified metadata
    fmt = data.get("format", {})
    duration = float(fmt.get("duration", 0)) if fmt.get("duration") else None
    container = fmt.get("format_name")
    bitrate = int(fmt.get("bit_rate")) if fmt.get("bit_rate") else None
    video_codec = None
    audio_codecs: list[str] = []
    width = height = None
    for s in data.get("streams", []):
        if s.get("codec_type") == "video" and video_codec is None:
            video_codec = s.get("codec_name")
            width = s.get("width")
            height = s.get("height")
        elif s.get("codec_type") == "audio":
            if s.get("codec_name"):
                audio_codecs.append(s.get("codec_name"))
    return {
        "duration": duration,
        "container": container,
        "video_codec": video_codec,
        "audio_codecs": audio_codecs,
        "width": width,
        "height": height,
        "bitrate": bitrate,
        "streams_json": json.dumps(data),
    }
