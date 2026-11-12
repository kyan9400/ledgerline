#!/usr/bin/env python3
"""The demo video and GIF from record.mjs's frames (needs ffmpeg).

    python make_video.py <out dir of record.mjs> <docs/demo>

- ledgerline-demo.mp4: the whole walk-through (the outage as a time-lapse, 30x).
- ledgerline-outage.gif: just the outage, smaller, for the README.
"""

from __future__ import annotations

import subprocess
import sys
from pathlib import Path

# Seconds each frame stays on screen, per part.
HOLD = {"1-live": 1.2, "2-outage": 0.1, "3-after": 1.0, "4-trace": 1.4}


def concat_list(frames: list[Path], hold: dict[Path, float], path: Path) -> None:
    lines = []
    for f in frames:
        lines += [f"file '{f.as_posix()}'", f"duration {hold[f]}"]
    lines.append(f"file '{frames[-1].as_posix()}'")  # the concat demuxer drops the last duration otherwise
    path.write_text("\n".join(lines) + "\n")


def run(*args: str) -> None:
    subprocess.run(["ffmpeg", "-y", "-loglevel", "error", *args], check=True)


def main(out: Path, dest: Path) -> None:
    frames, hold = [], {}
    for part, seconds in HOLD.items():
        for f in sorted((out / "frames" / part).glob("*.png")):
            frames.append(f)
            hold[f] = seconds
    lst = out / "all.txt"
    concat_list(frames, hold, lst)
    run("-f", "concat", "-safe", "0", "-i", str(lst), "-fps_mode", "vfr", "-vf", "scale=1440:-2,format=yuv420p",
        "-c:v", "libx264", "-crf", "24", "-preset", "slow", "-movflags", "+faststart", str(dest / "ledgerline-demo.mp4"))

    outage = [f for f in frames if f.parent.name == "2-outage"]
    lst = out / "outage.txt"
    concat_list(outage, {f: 0.1 for f in outage}, lst)
    palette = out / "palette.png"
    scale = "fps=10,scale=960:-1:flags=lanczos"
    run("-f", "concat", "-safe", "0", "-i", str(lst), "-vf", f"{scale},palettegen=max_colors=128", str(palette))
    run("-f", "concat", "-safe", "0", "-i", str(lst), "-i", str(palette), "-lavfi",
        f"{scale}[x];[x][1:v]paletteuse=dither=bayer:bayer_scale=4", "-loop", "0", str(dest / "ledgerline-outage.gif"))
    for name in ("ledgerline-demo.mp4", "ledgerline-outage.gif"):
        print(name, f"{(dest / name).stat().st_size / 1e6:.1f} MB")


if __name__ == "__main__":
    main(Path(sys.argv[1]), Path(sys.argv[2]))
