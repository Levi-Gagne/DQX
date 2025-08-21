from __future__ import annotations

import re
import time as _time
from functools import wraps
from typing import Dict, List, Optional

# Use YOUR color class exactly as provided
from profilon.utils.color import Color


class Console:
    """
    Color tokens, banners, and a timer decorator using your Color class directly.

    Usage:
        print(f"{Console.INFO}Starting…")
        Console.print_banner("h2", "Loading rules")

        @Console.banner_timer("Run DQX Checks", kind="h1")
        def run(): ...
    """

    # No toggles/heuristics — always emit your ANSI codes
    _RESET = Color.r

    # ----- tokens -----
    # Direct references to Color variables (not string names)
    BRACKET_STYLE: List[str] = [Color.b, Color.sky_blue]
    TAG_STYLES: Dict[str, List[str]] = {
        "ERROR":      [Color.b, Color.candy_red, Color.r],
        "WARNING":    [Color.b, Color.yellow, Color.r],
        "WARN":       [Color.b, Color.yellow, Color.r],
        "INFO":       [Color.b, Color.sky_blue, Color.r],
        "SUCCESS":    [Color.b, Color.green, Color.r],
        "VALIDATION": [Color.b, Color.ivory, Color.r],
        "DEBUG":      [Color.r],
        # added for your notebooks:
        "LOADER":     [Color.b, Color.sky_blue, Color.r],
        "SKIP":       [Color.b, Color.ivory, Color.r],
        "DEDUPE":     [Color.b, Color.moccasin, Color.r],
    }

    @classmethod
    def _codes(cls, codes: List[str]) -> str:
        # codes are already ANSI strings from Color
        return "".join(c for c in codes if isinstance(c, str))

    @classmethod
    def token(cls, label: str) -> str:
        lab = (label or "").upper()
        br  = cls._codes(cls.BRACKET_STYLE)
        lb  = cls._codes(cls.TAG_STYLES.get(lab, [Color.r]))
        return f"{br}[{cls._RESET}{lb}{lab}{cls._RESET}{br}]{cls._RESET}"

    _TOKEN = re.compile(r"\{([A-Za-z0-9_]+)\}")
    @classmethod
    def tagify(cls, s: str) -> str:
        """Replace `{TOKEN}` with a colored token."""
        return cls._TOKEN.sub(lambda m: cls.token(m.group(1)), s)

    # ----- banners -----
    # Use direct Color codes for border/text
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
        k = kind.lower()
        cfg = cls.BANNERS.get(k)
        if not cfg:
            raise KeyError(f"Unknown banner kind '{kind}'. Known: {sorted(cls.BANNERS)}")
        w = int(width or cfg["width"])
        border = cfg["border"]
        text   = cfg["text"]

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
                    h, rem = divmod(elapsed, 3600)
                    m, s = divmod(rem, 60)
                    dur = f"{int(h)}h {int(m)}m {s:.2f}s" if h else (f"{int(m)}m {s:.2f}s" if m else f"{s:.2f}s")
                    print(cls.banner(kind, f"END {text} — finished in {dur}"))
            return wrapped
        return deco


# export convenience constants like Console.ERROR / Console.INFO / Console.LOADER
for _lab in list(Console.TAG_STYLES):
    setattr(Console, _lab, Console.token(_lab))