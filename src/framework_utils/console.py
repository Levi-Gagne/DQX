# src/dqx/utils/console.py

from __future__ import annotations

import re
import time as _time
from functools import wraps
from typing import Dict, List, Optional, Iterable

from framework_utils.color import Color


class Console:
    """
    Simple colorized console helpers that use your Color codes directly.

    Usage:
        print(f"{Console.INFO}Starting…")
        Console.print_banner("h2", "Loading rules")

        @Console.banner_timer("Run DQX Checks", kind="h1")
        def run(): ...
    """

    # ----- quick helpers -----
    _RESET = Color.r

    @staticmethod
    def _join(parts: Iterable[str]) -> str:
        return "".join(p for p in parts if isinstance(p, str) and p)

    @staticmethod
    def _format_elapsed(seconds: float) -> str:
        s = float(seconds)
        m, s = divmod(s, 60.0)
        h, m = divmod(int(m), 60)
        return f"{h}h {m}m {s:.2f}s" if h else (f"{m}m {s:.2f}s" if m else f"{s:.2f}s")

    # ----- tag tokens -----
    # Bracket color/style
    _BR_LEFT = Color.b + Color.sky_blue
    _BR_RIGHT = Color.b + Color.sky_blue

    # Map tag -> [styles...] using Color.* constants directly
    TAG_STYLES: Dict[str, List[str]] = {
        "ERROR":      [Color.b, Color.candy_red, Color.r],
        "WARNING":    [Color.b, Color.yellow,   Color.r],
        "WARN":       [Color.b, Color.yellow,   Color.r],
        "INFO":       [Color.b, Color.sky_blue, Color.r],
        "SUCCESS":    [Color.b, Color.green,    Color.r],
        "VALIDATION": [Color.b, Color.ivory,    Color.r],
        "DEBUG":      [Color.b, Color.soft_gray, Color.r],

        # Added for your notebooks:
        "LOADER":     [Color.b, Color.sky_blue,   Color.r],
        "SKIP":       [Color.b, Color.ivory,      Color.r],
        "DEDUPE":     [Color.b, Color.moccasin,   Color.r],
    }

    @classmethod
    def token(cls, label: str) -> str:
        lab = (label or "").upper()
        styles = cls.TAG_STYLES.get(lab, [Color.b, Color.white, Color.r])
        # [BR][RESET][STYLE TAG STYLE][RESET][BR]
        return (
            f"{cls._BR_LEFT}[{cls._RESET}"
            f"{cls._join(styles)}{lab}{cls._RESET}"
            f"{cls._BR_RIGHT}]{cls._RESET}"
        )

    _TOKEN_RE = re.compile(r"\{([A-Za-z0-9_]+)\}")
    @classmethod
    def tagify(cls, s: str) -> str:
        """Replace `{TAG}` with a colored token."""
        return cls._TOKEN_RE.sub(lambda m: cls.token(m.group(1)), s or "")

    # ----- banners -----
    BANNERS: Dict[str, Dict[str, object]] = {
        "app": {"width": 72, "border": Color.sky_blue,  "text": Color.ivory},
        "h1":  {"width": 72, "border": Color.aqua_blue, "text": Color.ivory},
        "h2":  {"width": 64, "border": Color.green,     "text": Color.ivory},
        "h3":  {"width": 56, "border": Color.sky_blue,  "text": Color.ivory},
        "h4":  {"width": 48, "border": Color.aqua_blue, "text": Color.ivory},
        "h5":  {"width": 36, "border": Color.green,     "text": Color.ivory},
        "h6":  {"width": 24, "border": Color.ivory,     "text": Color.sky_blue},
    }

    @classmethod
    def banner(cls, kind: str, title: str, *, width: Optional[int] = None) -> str:
        k = (kind or "h2").lower()
        cfg = cls.BANNERS.get(k)
        if not cfg:
            raise KeyError(f"Unknown banner kind '{kind}'. Known: {sorted(cls.BANNERS)}")
        w = int(width or cfg["width"])
        border = str(cfg["border"])
        text   = str(cfg["text"])

        bar = f"{border}{Color.b}" + "═" * w + cls._RESET
        inner_width = max(0, w - 4)
        title_line = (
            f"{border}{Color.b}║ "
            f"{text}{Color.b}{title.center(inner_width)}{cls._RESET}"
            f"{border}{Color.b} ║{cls._RESET}"
        )
        return f"\n{bar}\n{title_line}\n{bar}"

    @classmethod
    def print_banner(cls, kind: str, title: str, *, width: Optional[int] = None) -> None:
        print(cls.banner(kind, title, width=width))

    @classmethod
    def banner_timer(cls, title: Optional[str] = None, *, kind: str = "app"):
        """Decorator that prints START/END banners with elapsed time."""
        def deco(fn):
            text = title or fn.__name__.replace("_", " ").title()
            @wraps(fn)
            def wrapped(*args, **kwargs):
                print(cls.banner(kind, f"START {text}"))
                t0 = _time.perf_counter()
                try:
                    return fn(*args, **kwargs)
                finally:
                    elapsed = _time.perf_counter() - t0
                    print(cls.banner(kind, f"END {text} — finished in {cls._format_elapsed(elapsed)}"))
            return wrapped
        return deco


# Export convenience constants like Console.ERROR / Console.INFO / Console.LOADER...
for _lab in list(Console.TAG_STYLES):
    setattr(Console, _lab, Console.token(_lab))