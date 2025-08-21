So its not actually applying the color


Here: 
    TAG_STYLES: Dict[str, List[str]] = {
        "ERROR":      ["b", "candy_red", "r"],
        "WARNING":    ["b", "yellow", "r"],
        "WARN":       ["b", "yellow", "r"],
        "INFO":       ["b", "sky_blue", "r"],
        "SUCCESS":    ["b", "green", "r"],
        "VALIDATION": ["b", "ivory", "r"],
        "DEBUG":      ["r"],
        # added for your notebooks:
        "LOADER":     ["b", "sky_blue", "r"],
        "SKIP":       ["b", "ivory", "r"],
        "DEDUPE":     ["b", "moccasin", "r"],
    }

as the color definintion are not refernce

shouldt it be like this?


        "ERROR":      ["b", {Color.candy_red}, "r"],
        "WARNING":    ["b", {Color.yellow}, "r"],
