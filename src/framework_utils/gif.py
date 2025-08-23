# ================================================================================
# Module: utils/gifConfig.py
# Description:
#   This module provides the GifUtils class for selecting GIF URLs from a predefined list.
#   It supports returning a random GIF or filtering by a keyword in the GIF name. If no match
#   is found or an error occurs, a fallback GIF is returned.
#
#   Workflow:
#   1. GIF Dictionary:
#      - A class‐level list of dictionaries, each containing a "name" and "url" key.
#   2. GIF Selection:
#      - The get_random_gif method optionally filters the dictionary by a candidate_name substring.
#      - If filtered entries exist, it returns one at random.
#      - Otherwise, selects any random GIF from the full list.
#      - On exception or if the list is empty, returns a hardcoded fallback GIF URL.
#
# Usage:
#   - Call GifUtils.get_random_gif() for a random GIF.
#   - Call GifUtils.get_random_gif("error") to get a random GIF whose name contains "error".
#
# Author: Levi Gagne
# Created Date: 2025-04-16
# Last Modified: 2025-04-16
# ================================================================================

import random  # For random selection from lists
from typing import Optional  # For optional parameter typing


class GifUtils:
    """
    Class: GifUtils
    Description:
      Utility class for retrieving random GIF URLs from a predefined dictionary.
      Supports optional name-based filtering and graceful fallback on error.
    """
    
    # Predefined list of GIF entries. Each entry has a descriptive name and a direct URL.
    gif_dictionary = [
        {
            "name": "failure",
            "url": "https://media2.giphy.com/media/v1.Y2lkPTc5MGI3NjExYms2Y2J3YjBuZGJxMWZpaXJzbXF0emxsYmI1eWRnbmRrNXJhM2VsNyZlcD12MV9pbnRlcm5hbF9naWZfYnlfaWQmY3Q9Zw/A1SxC5HRrD3MY/giphy.gif"
        },
        {
            "name": "Smashing Computer",
            "url": "https://media1.giphy.com/media/v1.Y2lkPTc5MGI3NjExNHV6bzB2OHY1ZTV2YjRyMTljdmp1N2w2cHAyNmtkenpvb28xNTJtaCZlcD12MV9pbnRlcm5hbF9naWZfYnlfaWQmY3Q9Zw/9ZOyRdXL7ZVKg/giphy.gif"
        },
        {
            "name": "SNL Fix",
            "url": "https://media0.giphy.com/media/v1.Y2lkPTc5MGI3NjExaXhzdXF4ejN4OGlucnVtOXFlcmd0bzM5MGVoYmtkdTc3dWEzMTRybiZlcD12MV9pbnRlcm5hbF9naWZfYnlfaWQmY3Q9Zw/PnpkimJ5mrZRe/giphy.gif"
        },
        {
            "name": "Why Doesn't This Work",
            "url": "https://media2.giphy.com/media/v1.Y2lkPTc5MGI3NjExbmI4aHUxeDJxbTF2aTMwNmljZ2NhbmdkcW5sbDF0aTh3ZjVnbXlpNyZlcD12MV9pbnRlcm5hbF9naWZfYnlfaWQmY3Q9Zw/Rkis28kMJd1aE/giphy.gif"
        },
        {
            "name": "Computer Fire",
            "url": "https://media0.giphy.com/media/v1.Y2lkPTc5MGI3NjExdGp0enF3ZzlwYnNsamtzc2thZmdjNmMzaDcwdXRrdm53am8zN2sybSZlcD12MV9pbnRlcm5hbF9naWZfYnlfaWQmY3Q9Zw/CZZFrvvfdaYcsOyyh0/giphy.gif"
        },
        {
            "name": "ERROR",
            "url": "https://media3.giphy.com/media/v1.Y2lkPTc5MGI3NjExcjJmendqZHk4cGp3aDZ6aGJyZjZlZ2tqZmF2bjVhdGFhZnIzOGw3bSZlcD12MV9pbnRlcm5hbF9naWZfYnlfaWQmY3Q9Zw/3o7WTDH9gYo71TurPq/giphy.gif"
        },
        {
            "name": "Processing . . . Attempting to Care",
            "url": "https://media1.giphy.com/media/v1.Y2lkPTc5MGI3NjExYng3dWtmeWQwaGlyNXFoeHB1Zmx0aGtwNXZ3dm11Njg0Z2s4NDFsOSZlcD12MV9pbnRlcm5hbF9naWZfYnlfaWQmY3Q9Zw/l46Czzp0KEHSO7OdG/giphy.gif"
        },
        {
            "name": "Data Engineering/Science Assist",
            "url": "https://media2.giphy.com/media/v1.Y2lkPTc5MGI3NjExaTg4MGpoMXdjMmxwbnljNTR1YmticmV6bDcyYnh0NzFpZHJjNW9tMCZlcD12MV9pbnRlcm5hbF9naWZfYnlfaWQmY3Q9Zw/fryY00CO4xCz4uJuDQ/giphy.gif"
        },
        {
            "name": "Can't be told data is wrong - if you dont track it",
            "url": "https://media0.giphy.com/media/v1.Y2lkPTc5MGI3NjExMDltM2dvNnBwbGsyNm9kcTFvc2gzMHlheDRzOHA0M3F4YzB2Y2ExZyZlcD12MV9pbnRlcm5hbF9naWZfYnlfaWQmY3Q9Zw/GbH8vRmrNHdVZhouBt/giphy.gif"
        },
        {
            "name": "Racing Horse",
            "url": "https://media4.giphy.com/media/v1.Y2lkPTc5MGI3NjExNzE0d3B5Z3B6dDR6czM3djl0dnY3em92a2ZoeDZseGxhaHUxcGRkcyZlcD12MV9pbnRlcm5hbF9naWZfYnlfaWQmY3Q9Zw/MFz8CQ1ufshKd4oeUx/giphy.gif"
        },
        {
            "name": "Dancing Computer",
            "url": "https://media3.giphy.com/media/v1.Y2lkPTc5MGI3NjExOHYxZ2tkcHJ6ZHcyMXd6dDUzZGs4c2VwZnUyZTZ0N2RxYzM0a2M2YiZlcD12MV9pbnRlcm5hbF9naWZfYnlfaWQmY3Q9Zw/MT5UUV1d4CXE2A37Dg/giphy.gif"
        }
    ]
    
    @classmethod
    def get_random_gif(cls, candidate_name: Optional[str] = None) -> str:
        """
        Returns a random GIF URL from gif_dictionary.
        
        If candidate_name is provided:
            - Filter gif_dictionary entries whose 'name' contains candidate_name (case-insensitive).
            - If any filtered entries exist, return one at random.
        If candidate_name is not provided or no filtered entries found:
            - Return a random URL from the full gif_dictionary.
        On error or empty dictionary:
            - Log the exception.
            - Return a fallback GIF URL.
        
        Parameters:
            candidate_name (Optional[str]): Keyword to filter GIF names.
        
        Returns:
            str: URL of the selected GIF.
        """
        try:
            # If a filter term is provided, select matching gifs.
            if candidate_name:
                filtered = [
                    gif for gif in cls.gif_dictionary
                    if candidate_name.lower() in gif["name"].lower()
                ]
                # If any matches, return a random match's URL.
                if filtered:
                    return random.choice(filtered)["url"]
            
            # If no gifs exist in the dictionary, signal error.
            if not cls.gif_dictionary:
                raise ValueError("No GIFs available in the dictionary.")
            
            # Return a random GIF URL from the full dictionary.
            return random.choice(cls.gif_dictionary)["url"]
        
        except Exception as e:
            # Define a hardcoded fallback GIF URL.
            fallback = "https://media.giphy.com/media/JIX9t2j0ZTN9S/giphy.gif"
            # Log error and fallback selection.
            print(f"Error in get_random_gif: {e}. Returning fallback GIF: {fallback}")
            return fallback