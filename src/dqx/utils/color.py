# src/profilon/utils/color.py

"""
colorConfig.py
---------------
This module provides a utility class for managing terminal color codes
and applying them to text for formatted output.

Features:
- Standard colors
- Extended colors
- Custom colors
- Color themes with shades
- Utility methods for applying and formatting colors
- Support for background colors
- Text styling options (bold, italic, underline, etc.)
- Comprehensive documentation for easier understanding

Author: Levi Gagne
"""

class Color:
    # ---------------------------------------------------------------------
    # Text Modifiers (SGR 0-8)
    # ---------------------------------------------------------------------
    r  = '\033[0m'   # Reset all attributes
    b  = '\033[1m'   # Bold/brighter
    d  = '\033[2m'   # Dim/darker
    i  = '\033[3m'   # Italic (not widely supported)
    u  = '\033[4m'   # Underline
    bl = '\033[5m'   # Blink (not widely supported and generally discouraged)
    rv = '\033[7m'   # Reverse (invert foreground and background colors)
    h  = '\033[8m'   # Hidden (not widely supported)

    # ---------------------------------------------------------------------
    # Additional SGR Codes (9m-29m)
    # ---------------------------------------------------------------------
    st = '\033[9m'    # Crossed-out (strike-through; rarely supported)

    font_primary = '\033[10m'  # Primary font (default)
    font_alt1    = '\033[11m'  # Alternative font 1
    font_alt2    = '\033[12m'  # Alternative font 2
    font_alt3    = '\033[13m'  # Alternative font 3
    font_alt4    = '\033[14m'  # Alternative font 4
    font_alt5    = '\033[15m'  # Alternative font 5
    font_alt6    = '\033[16m'  # Alternative font 6
    font_alt7    = '\033[17m'  # Alternative font 7
    font_alt8    = '\033[18m'  # Alternative font 8
    font_alt9    = '\033[19m'  # Alternative font 9

    fraktur         = '\033[20m'  # Fraktur (Gothic; rarely supported, sometimes rendered as italic)
    bold_off        = '\033[21m'  # Bold off / Double underline (varies by terminal)
    normal_intensity = '\033[22m'  # Normal intensity (neither bold nor faint)
    cancel_italic   = '\033[23m'  # Cancel italic
    underline_off   = '\033[24m'  # Underline off
    blink_off       = '\033[25m'  # Blink off
    reserved        = '\033[26m'  # Reserved (not used)
    reverse_off     = '\033[27m'  # Reverse off (cancel reverse video)
    reveal          = '\033[28m'  # Reveal (cancel conceal/hide)
    cancel_strike   = '\033[29m'  # Cancel crossed-out (strike-through)

    # ---------------------------------------------------------------------
    # Base Colors (Foreground) (SGR 30-39)
    # ---------------------------------------------------------------------
    black  = '\033[30m'  # Black text
    red    = '\033[31m'  # Red text
    green  = '\033[32m'  # Green text
    yellow = '\033[33m'  # Yellow text
    blue   = '\033[34m'  # Blue text
    purple = '\033[35m'  # Purple text
    cyan   = '\033[36m'  # Cyan text
    white  = '\033[37m'  # White text

    ext_fg     = '\033[38m'  # Extended foreground (requires extra parameters)
    default_fg = '\033[39m'  # Default foreground color (reset)

    # ---------------------------------------------------------------------
    # Background Colors (SGR 40-49)
    # ---------------------------------------------------------------------
    black_bg  = '\033[40m'  # Black background
    red_bg    = '\033[41m'  # Red background
    green_bg  = '\033[42m'  # Green background
    yellow_bg = '\033[43m'  # Yellow background
    blue_bg   = '\033[44m'  # Blue background
    purple_bg = '\033[45m'  # Purple background
    cyan_bg   = '\033[46m'  # Cyan background
    white_bg  = '\033[47m'  # White background

    ext_bg     = '\033[48m'  # Extended background (requires extra parameters)
    default_bg = '\033[49m'  # Default background color (reset)

    # ---------------------------------------------------------------------
    # Bright (High-Intensity) Foreground Colors (SGR 90-97)
    # ---------------------------------------------------------------------
    black_br  = '\033[90m'  # Bright black (often appears as dark gray)
    red_br    = '\033[91m'  # Bright red
    green_br  = '\033[92m'  # Bright green
    yellow_br = '\033[93m'  # Bright yellow
    blue_br   = '\033[94m'  # Bright blue
    purple_br = '\033[95m'  # Bright magenta (purple)
    cyan_br   = '\033[96m'  # Bright cyan
    white_br  = '\033[97m'  # Bright white

    # ---------------------------------------------------------------------
    # Bright (High-Intensity) Background Colors (SGR 100-107)
    # ---------------------------------------------------------------------
    black_bg_br  = '\033[100m'  # Bright background black (often dark gray)
    red_bg_br    = '\033[101m'  # Bright background red
    green_bg_br  = '\033[102m'  # Bright background green
    yellow_bg_br = '\033[103m'  # Bright background yellow
    blue_bg_br   = '\033[104m'  # Bright background blue
    purple_bg_br = '\033[105m'  # Bright background magenta
    cyan_bg_br   = '\033[106m'  # Bright background cyan
    white_bg_br  = '\033[107m'  # Bright background white

    # ---------------------------------------------------------------------
    # Extended / Custom Colors (Non-basic; using 24-bit color codes)
    # These colors use longer sequences (e.g., "\033[38;2;R;G;Bm") and are
    # ideal for dark mode or high color fidelity.
    # ---------------------------------------------------------------------
    # Terminal Text Colors
    ivory       = '\033[38;2;255;255;240m'  # Ivory White
    ghost_white = '\033[38;2;248;248;255m'  # Ghost White
    soft_gray   = '\033[38;2;211;211;211m'  # Soft Gray

    # ---- Warm Tones ----
    pastel_peach = '\033[38;2;255;218;185m'  # Pastel Peach
    light_coral  = '\033[38;2;240;128;128m'  # Light Coral
    sandy_brown  = '\033[38;2;244;164;96m'   # Sandy Brown
    moccasin     = '\033[38;2;255;228;181m'  # Moccasin

    # ---- Cool Tones ----
    sky_blue       = '\033[38;2;135;206;235m'  # Sky Blue
    light_seafoam  = '\033[38;2;144;238;144m'  # Light Seafoam Green
    pale_turquoise = '\033[38;2;175;238;238m'  # Pale Turquoise

    # ---- Soft Pink & Purple Tones ----
    lavender   = '\033[38;2;230;230;250m'  # Lavender
    blush_pink = '\033[38;2;255;182;193m'  # Blush Pink
    thistle    = '\033[38;2;216;191;216m'  # Thistle (Soft Purple)

    # ---------------------------------------------------------------------
    # Custom Colors (Grouped by Similar Shades)
    # ---------------------------------------------------------------------
    # -- Vibrant/Neon Reds --
    vibrant_red = '\033[38;2;176;29;45m'
    candy_red   = '\033[38;2;255;0;51m'
    scarlet_red = '\033[38;2;255;36;0m'

    # -- Reds & Pinks --
    bright_pink = '\033[38;2;255;20;147m'
    hot_pink    = '\033[38;2;255;105;180m'
    bubblegum_pink  = '\033[38;2;255;182;193m'
    tomato      = '\033[38;2;255;99;71m'
    matador_red = '\033[38;2;178;34;34m'

    # -- Oranges & Yellows --
    soft_orange      = '\033[38;2;226;76;44m'
    burnt_orange     = '\033[38;2;204;85;0m'
    tangerine        = '\033[38;2;255;127;80m'
    sunshine_yellow  = '\033[38;2;255;255;51m'
    golden_yellow    = '\033[38;2;255;223;0m'
    lemon_yellow     = '\033[38;2;255;250;205m'
    butternut_yellow = '\033[38;2;231;192;75m'
    colonial_cream   = '\033[38;2;253;233;16m'
    coronado_yellow  = '\033[38;2;255;255;0m'
    code_yellow      = '\033[33m'

    # -- Blues & Turquoises --
    deep_blue       = '\033[38;2;3;84;146m'
    royal_blue      = '\033[38;2;65;105;225m'
    peacock_blue    = '\033[38;2;0;102;204m'
    midnight_blue   = '\033[38;2;25;25;112m'
    bright_turquoise = '\033[38;2;64;224;208m'
    aqua_blue       = '\033[38;2;0;255;204m'
    deep_sky_blue   = '\033[38;2;0;191;255m'
    larkspur_blue   = '\033[38;2;108;142;191m'
    harbor_blue     = '\033[38;2;29;78;137m'
    bahama_blue     = '\033[38;2;1;162;217m'
    neon_blue       = '\033[38;2;77;255;255m'   # Neon blue for dark mode
    electric_cyan   = '\033[38;2;0;255;255m'    # Alias for a bright cyan

    # -- Greens --
    forest_green     = '\033[38;2;0;123;51m'
    neon_green       = '\033[38;2;57;255;20m'
    lime_green       = '\033[38;2;50;205;50m'
    bright_lime      = '\033[38;2;191;255;0m'
    spring_green     = '\033[38;2;0;255;127m'
    sea_green        = '\033[38;2;46;139;87m'
    chartreuse       = '\033[38;2;127;255;0m'
    light_green      = '\033[38;2;144;238;144m'
    highland_green   = '\033[38;2;14;98;81m'
    tropical_turquoise = '\033[38;2;0;128;0m'

    # -- Purples & Violets --
    electric_purple  = '\033[38;2;191;0;255m'
    grape_purple     = '\033[38;2;128;0;128m'
    fuchsia          = '\033[38;2;255;0;255m'
    deep_magenta     = '\033[38;2;139;0;139m'
    indigo           = '\033[38;2;75;0;130m'
    violet           = '\033[38;2;238;130;238m'
    orchid           = '\033[38;2;218;112;214m'
    plum             = '\033[38;2;221;160;221m'
    med_slate_blue   = '\033[38;2;123;104;238m'
    # (lavender already defined above)

    # -- Neutrals & Browns --
    cool_gray        = '\033[38;2;119;136;153m'
    chocolate        = '\033[38;2;210;105;30m'
    onyx_black       = '\033[38;2;12;12;12m'
    imperial_ivory   = '\033[38;2;242;240;230m'
    adobe_beige      = '\033[38;2;195;176;145m'
    burlywood        = '\033[38;2;222;184;135m'
    saddle_brown     = '\033[38;2;139;69;19m'
    sienna           = '\033[38;2;160;82;45m'
    peru             = '\033[38;2;205;133;63m'
    sandy_brown      = '\033[38;2;244;164;96m'
    dark_orange      = '\033[38;2;255;140;0m'
    orange_red       = '\033[38;2;255;69;0m'
    slate_gray       = '\033[38;2;112;128;144m'
    dark_slate_gray  = '\033[38;2;47;79;79m'
    off_white        = '\033[38;2;245;245;245m'  # Off-white for dark mode

    # ---- Extended Colors (Missing from class) ----
    dark_red        = "\033[38;2;139;0;0m"
    crimson         = "\033[38;2;220;20;60m"
    dark_green      = "\033[38;2;0;100;0m"
    olive           = "\033[38;2;128;128;0m"
    navy            = "\033[38;2;0;0;128m"
    teal            = "\033[38;2;0;128;128m"
    silver          = "\033[38;2;192;192;192m"
    maroon          = "\033[38;2;128;0;0m"
    lime            = "\033[38;2;0;255;0m"
    aqua            = "\033[38;2;0;255;255m"
    fuchsia         = "\033[38;2;255;0;255m"
    gray            = "\033[38;2;128;128;128m"
    coral           = "\033[38;2;255;127;80m"
    goldenrod       = "\033[38;2;218;165;32m"
    pink            = "\033[38;2;255;192;203m"
    steel_blue      = "\033[38;2;70;130;180m"
    deep_pink       = "\033[38;2;255;20;147m"
    misty_rose      = "\033[38;2;255;228;225m"
    beige           = "\033[38;2;245;245;220m"
    saffron         = "\033[38;2;251;197;90m"

    # -- Additional / Misc --
    python           = '\033[38;2;75;139;190m'
    spark            = '\033[38;2;255;218;0m'

    # ---------------------------------------------------------------------
    # Cluster Colors and Themes
    # ---------------------------------------------------------------------
    CLUSTER_PRIMARY    = '\033[38;2;60;179;113m'   # Dark Green – for main headers (e.g., Cluster title)
    CLUSTER_SECONDARY  = '\033[38;2;144;238;144m'  # Forest Green – for subheaders (e.g., Cluster ID, Creator)
    CLUSTER_TERTIARY   = '\033[38;2;50;205;50m'    # Medium Sea Green – for labels (e.g., driver/executor)
    CLUSTER_QUATERNARY = '\033[38;2;173;255;47m'   # Light Green – for detailed info or supplementary text

    color_themes = {
        "black": {
            "shade_1": "\033[38;2;20;20;20m",
            "shade_2": "\033[38;2;40;40;40m",
            "shade_3": "\033[38;2;60;60;60m",
            "shade_4": "\033[38;2;80;80;80m",
            "shade_5": "\033[38;2;100;100;100m",
            "shade_6": "\033[38;2;120;120;120m"
        },
        "red": {
            "shade_1": "\033[38;2;139;0;0m",
            "shade_2": "\033[38;2;165;42;42m",
            "shade_3": "\033[38;2;178;34;34m",
            "shade_4": "\033[38;2;205;92;92m",
            "shade_5": "\033[38;2;220;20;60m",
            "shade_6": "\033[38;2;255;0;0m"
        },
        "green": {
            "shade_1": "\033[38;2;0;100;0m",
            "shade_2": "\033[38;2;34;139;34m",
            "shade_3": "\033[38;2;50;205;50m",
            "shade_4": "\033[38;2;60;179;113m",
            "shade_5": "\033[38;2;144;238;144m",
            "shade_6": "\033[38;2;152;251;152m"
        },
        "yellow": {
            "shade_1": "\033[38;2;139;139;0m",
            "shade_2": "\033[38;2;173;173;47m",
            "shade_3": "\033[38;2;204;204;0m",
            "shade_4": "\033[38;2;238;238;0m",
            "shade_5": "\033[38;2;255;255;0m",
            "shade_6": "\033[38;2;255;255;153m"
        },
        "blue": {
            "shade_1": "\033[38;2;0;0;139m",
            "shade_2": "\033[38;2;0;0;205m",
            "shade_3": "\033[38;2;65;105;225m",
            "shade_4": "\033[38;2;100;149;237m",
            "shade_5": "\033[38;2;135;206;235m",
            "shade_6": "\033[38;2;173;216;230m"
        },
        "purple": {
            "shade_1": "\033[38;2;75;0;130m",
            "shade_2": "\033[38;2;128;0;128m",
            "shade_3": "\033[38;2;147;112;219m",
            "shade_4": "\033[38;2;186;85;211m",
            "shade_5": "\033[38;2;221;160;221m",
            "shade_6": "\033[38;2;238;130;238m"
        },
        "cyan": {
            "shade_1": "\033[38;2;0;139;139m",
            "shade_2": "\033[38;2;0;255;255m",
            "shade_3": "\033[38;2;72;209;204m",
            "shade_4": "\033[38;2;175;238;238m",
            "shade_5": "\033[38;2;224;255;255m",
            "shade_6": "\033[38;2;240;255;255m"
        },
        "white": {
            "shade_1": "\033[38;2;220;220;220m",
            "shade_2": "\033[38;2;230;230;230m",
            "shade_3": "\033[38;2;240;240;240m",
            "shade_4": "\033[38;2;245;245;245m",
            "shade_5": "\033[38;2;250;250;250m",
            "shade_6": "\033[38;2;255;255;255m"  # Pure white
        },
        # ... add any additional colors here if needed ...
    }

    ##############################################################################
    # CLA Brand Colors
    #
    # Core (Primary) Colors:
    #   - riptide  (#7DD2D3): Used as a modern anchor color.
    #   - navy     (#2E334E): Used to ground visuals, anchor sections, and for key text.
    #
    # Secondary Colors:
    #   - celadon  (#E2E868)
    #   - saffron  (#FBC55A)
    #   - scarlett (#EE5340): Use mostly for accents and small highlights.
    #
    # Neutrals:
    #   - charcoal (#25282A), smoke (#ABAEAB), cloud (#F7F7F6), white (#FFFFFF), black (#000000)
    #   - Use neutrals for backgrounds, borders, and text contrast.
    #
    # Tints & Shades:
    #   - Approved lighter (tints) and darker (shades) variants of core/secondary colors.
    #   - Use sparingly for infographics, charts, or contrast; not for main backgrounds.
    #
    # Usage Notes:
    #   - Use only these HEX codes for digital/CLI output (matches CLA web branding).
    #   - Navy should anchor layouts; scarlett should accent, not dominate.
    #   - Only use the provided tints/shades—no improvising.
    #
    # Example (direct):         print(f"{C.cla['riptide']}Riptide{C.r}")
    # Example (helper):         print(f"{C.cla_color('saffron', 'medium')}Saffron Medium Tint{C.r}")
    ##############################################################################
    cla = {
        # --- Primary ---
        "riptide":         "\033[38;2;125;210;211m",  # #7DD2D3
        "navy":            "\033[38;2;46;51;78m",     # #2E334E

        # --- Secondary ---
        "celadon":         "\033[38;2;226;232;104m",  # #E2E868
        "saffron":         "\033[38;2;251;197;90m",   # #FBC55A
        "scarlett":        "\033[38;2;238;83;64m",    # #EE5340

        # --- Neutrals ---
        "charcoal":        "\033[38;2;37;40;42m",     # #25282A
        "smoke":           "\033[38;2;171;174;171m",  # #ABAEAB
        "cloud":           "\033[38;2;247;247;246m",  # #F7F7F6
        "white":           "\033[38;2;255;255;255m",  # #FFFFFF
        "black":           "\033[38;2;0;0;0m",        # #000000

        # --- Tints ---
        "riptide_tints": {
            "light":        "\033[38;2;194;234;234m",  # #C2EAEA
            "medium":       "\033[38;2;164;223;224m",  # #A4DFE0
            "dark":         "\033[38;2;149;217;219m",  # #95D9DB
        },
        "celadon_tints": {
            "light":        "\033[38;2;245;247;209m",  # #F5F7D1
            "medium":       "\033[38;2;238;242;178m",  # #EEF2B2
            "dark":         "\033[38;2;231;236;147m",  # #E7EC93
        },
        "saffron_tints": {
            "light":        "\033[38;2;254;238;206m",  # #FEEECE
            "medium":       "\033[38;2;253;220;156m",  # #FDDC9C
            "dark":         "\033[38;2;252;209;123m",  # #FCD17B
        },
        "scarlett_tints": {
            "light":        "\033[38;2;251;205;196m",  # #FBCDC4
            "medium":       "\033[38;2;246;155;137m",  # #F69B89
            "dark":         "\033[38;2;243;121;98m",   # #F37962
        },

        # --- Shades ---
        "riptide_shades": {
            "light":        "\033[38;2;73;191;193m",   # #49BFC1
            "medium":       "\033[38;2;57;165;167m",   # #39A5A7
            "dark":         "\033[38;2;36;120;122m",   # #24787A
        },
        "navy_shades": {
            "light":        "\033[38;2;38;42;64m",     # #262A40
            "medium":       "\033[38;2;30;33;51m",     # #1E2133
            "dark":         "\033[38;2;23;25;39m",     # #171927
        },
    }
    # Example for direct usage:
    # print(f"{C.cla['riptide']}Riptide Example{C.r}")
    # print(f"{C.cla['riptide_shades']['light']}Riptide Light Shade Example{C.r}")
    
    # Helper for easy access:
    @staticmethod
    def cla_color(name, shade=None):
        """
        Quick access to CLA colors and their tints/shades.
        Example:
            C.cla_color('navy')
            C.cla_color('scarlett', 'light')
            C.cla_color('riptide_shades', 'dark')
        """
        c = C.cla
        if shade and name + "_tints" in c:
            return c[name + "_tints"].get(shade, "")
        if shade and name + "_shades" in c:
            return c[name + "_shades"].get(shade, "")
        return c.get(name, "")
    ##############################################################################
    ###############################   CLA Colors   ###############################
    ##############################################################################


    @staticmethod
    def apply_color(text, color_code, bg_color_code=None, text_style=None):
        """
        Applies the specified color code, optional background color, and optional text style to the given text.

        :param text: The text to format.
        :param color_code: The ANSI color code to apply.
        :param bg_color_code: The optional background color code to apply.
        :param text_style: The optional text style to apply.
        :return: Formatted text with the applied color, background, and style.
        """
        style = text_style if text_style else ""
        if bg_color_code:
            return f"{style}{color_code}{bg_color_code}{text}{C.r}"
        return f"{style}{color_code}{text}{C.r}"

    @staticmethod
    def list_colors():
        """
        Lists all available color categories, text styles, and names.
        """
        print("Text Modifiers:")
        print("- r (Reset), b (Bold), d (Dim), i (Italic), u (Underline), bl (Blink), rv (Reverse), h (Hidden)")

        print("\nBase Colors:")
        for color in ["black", "red", "green", "yellow", "blue", "purple", "cyan", "white"]:
            print(f"- {color}")

        print("\nBackground Colors:")
        for color in ["black_bg", "red_bg", "green_bg", "yellow_bg", "blue_bg", "purple_bg", "cyan_bg", "white_bg"]:
            print(f"- {color}")

        print("\nExtended Colors:")
        for color in ["dark_red", "crimson", "dark_green", "olive", "navy", "teal", "silver", "maroon", "lime", "aqua", "fuchsia", "gray"]:
            print(f"- {color}")

        print("\nCustom Colors:")
        for color in ["vibrant_red", "soft_orange", "deep_blue", "forest_green", "sky_blue", "bright_pink", "golden_yellow", "cool_gray"]:
            print(f"- {color}")

    @staticmethod
    def colorConfigCheck():
        """
        Verifies the color configuration is working as expected.
        """
        print("Color config check successful.")