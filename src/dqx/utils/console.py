# src/dqx/utils/console.py

from __future__ import annotations

import os
import sys
import time as _time
import re
from typing import Dict, List, Optional, Iterable
from functools import wraps

from utils.color import Color


class Console:
    """
    Color tokens, banners, and a timer decorator.

    Usage:
        print(f"{Console.INFO}Starting…")
        Console.print_banner("h2", "Loading rules")

        @Console.banner_timer("Run DQX Checks", kind="h1")
        def run(): ...
    """

    # enable colors iff NOT disabled and TTY likely present
    _ENABLED = (os.environ.get("NO_COLOR", "") == "") and bool(getattr(sys.stdout, "isatty", lambda: False)())
    _RESET   = getattr(Color, "r", "") if _ENABLED else ""

    # ----- internal helpers -----
    @classmethod
    def _code(cls, name: str) -> str:
        return getattr(Color, name, "") if cls._ENABLED else ""

    @classmethod
    def _codes(cls, names: Iterable[str]) -> str:
        if not cls._ENABLED:
            return ""
        return "".join(getattr(Color, n, "") for n in names if isinstance(n, str))

    @staticmethod
    def _format_elapsed(seconds: float) -> str:
        s = float(seconds)
        m, s = divmod(s, 60.0)
        h, m = divmod(int(m), 60)
        return f"{h}h {m}m {s:.2f}s" if h else (f"{m}m {s:.2f}s" if m else f"{s:.2f}s")

    # ----- tokens -----
    BRACKET_STYLE: List[str] = ["b", "sky_blue"]
    TAG_STYLES: Dict[str, List[str]] = {
        "ERROR":      ["b", "candy_red", "r"],
        "WARNING":    ["b", "yellow", "r"],
        "WARN":       ["b", "yellow", "r"],
        "INFO":       ["b", "sky_blue", "r"],
        "SUCCESS":    ["b", "green", "r"],
        "VALIDATION": ["b", "ivory", "r"],
        "DEBUG":      ["r"],
    }

    @classmethod
    def token(cls, label: str) -> str:
        lab = (label or "").upper()
        br  = cls._codes(cls.BRACKET_STYLE)
        lb  = cls._codes(cls.TAG_STYLES.get(lab, ["r"]))
        return f"{br}[{cls._RESET}{lb}{lab}{cls._RESET}{br}]{cls._RESET}"

    _TOKEN = re.compile(r"\{([A-Za-z0-9_]+)\}")
    @classmethod
    def tagify(cls, s: str) -> str:
        return cls._TOKEN.sub(lambda m: cls.token(m.group(1)), s)

    # ----- banners -----
    BANNERS: Dict[str, Dict[str, object]] = {
        "app": {"width": 72, "border": "sky_blue",  "text": "ivory"},
        "h1":  {"width": 72, "border": "aqua_blue", "text": "ivory"},
        "h2":  {"width": 64, "border": "green",     "text": "ivory"},
        "h3":  {"width": 56, "border": "sky_blue",  "text": "ivory"},
        "h4":  {"width": 48, "border": "aqua_blue", "text": "ivory"},
        "h5":  {"width": 36, "border": "green",     "text": "ivory"},
        "h6":  {"width": 24, "border": "ivory",     "text": "sky_blue"},
    }

    @classmethod
    def banner(cls, kind: str, title: str, *, shades: Optional[Dict[str, str]] = None, width: Optional[int] = None) -> str:
        k = kind.lower()
        cfg = cls.BANNERS.get(k)
        if not cfg:
            raise KeyError(f"Unknown banner kind '{kind}'. Known: {sorted(cls.BANNERS)}")
        w = int(width or cfg["width"])
        border_attr = str((shades or {}).get("border", cfg["border"]))
        text_attr   = str((shades or {}).get("text",   cfg["text"]))

        border = cls._code(border_attr)
        text   = cls._code(text_attr)

        bar = f"{border}{Color.b}" + "═" * w + cls._RESET
        inner_width = max(0, w - 4)
        title_line = (
            f"{border}{Color.b}║ "
            f"{text}{Color.b}{title.center(inner_width)}{cls._RESET}"
            f"{border}{Color.b} ║{cls._RESET}"
        )
        return f"\n{bar}\n{title_line}\n{bar}"

    @classmethod
    def print_banner(cls, kind: str, title: str, *, shades: Optional[Dict[str, str]] = None, width: Optional[int] = None) -> None:
        print(cls.banner(kind, title, shades=shades, width=width))

    @classmethod
    def banner_timer(cls, title: Optional[str] = None, *, kind: str = "app", shades: Optional[Dict[str, str]] = None):
        """Decorator that prints START/END banners with elapsed time."""
        def deco(fn):
            text = title or fn.__name__.replace("_", " ").title()
            @wraps(fn)
            def wrapped(*args, **kwargs):
                print(cls.banner(kind, f"START {text}", shades=shades))
                t0 = _time.perf_counter()
                try:
                    return fn(*args, **kwargs)
                finally:
                    elapsed = _time.perf_counter() - t0
                    print(cls.banner(kind, f"END {text} — finished in {cls._format_elapsed(elapsed)}", shades=shades))
            return wrapped
        return deco


# export convenience constants like Console.ERROR / Console.INFO
for _lab in list(Console.TAG_STYLES):
    setattr(Console, _lab, Console.token(_lab))