"""
Module: utils/environmentConfig.py
Description:
    This module provides the EnvironmentConfig class which encapsulates configuration details
    fetched from OS environment variables. It retrieves critical environment variables:
      - ENV: The current deployment environment (e.g., development, production).
      - TIMEZONE: The primary timezone for the application.
      - LOCAL_TIMEZONE: The local timezone for system operations.
      
    The EnvironmentConfig class validates the presence of these variables, provides default values
    for timezone settings when necessary, and offers methods to print and access these configuration values.

Usage:
    - Instantiate EnvironmentConfig to automatically load and validate environment settings.
    - Use the print_environment_info() method to output the variables in a colorized, structured format.
    - Access the configuration details via the 'environment' and 'environment_vars' properties.

Note:
    - This module leverages the utils.colorConfig module (imported as C) to format printed output.

Author: Levi Gagne
Created Date: 2025-03-25
Last Modified: 2025-04-16
"""

import os  # For interacting with operating system environment variables.
from typing import Optional  # For type annotations.
from zoneinfo import ZoneInfo  # For timezone support if needed.

from utils.colorConfig import C # Import custom color configuration for terminal output.


class EnvironmentConfig:
    """
    Class: EnvironmentConfig
    Description:
        Encapsulates and validates critical environment variables needed for the application's
        configuration. This class retrieves the ENV, TIMEZONE, and LOCAL_TIMEZONE variables,
        sets defaults if necessary, and provides methods for printing and accessing these settings.
    """
    
    def __init__(self) -> None:
        """
        Initialize the EnvironmentConfig instance.
        
        Process:
            - Fetches the "ENV" environment variable; raises an error if missing.
            - Retrieves the LOCAL_TIMEZONE environment variable; defaults to "UTC" with a warning if missing.
            - Retrieves the TIMEZONE environment variable; defaults to "UTC" with a warning if missing.
            - Aggregates these variables in a dictionary for later access or printing.
        """
        # Retrieve the 'ENV' variable from the OS environment.
        env: Optional[str] = os.getenv("ENV")
        # Verify that the ENV variable is set; otherwise, raise an error with a colorized message.
        if not env:
            raise EnvironmentError(f"{C.b}{C.red}ENV environment variable is not set{C.r}")
        self._env: str = env

        # Retrieve the 'LOCAL_TIMEZONE' variable; if it is not set, warn and default to "UTC".
        local_tz: Optional[str] = os.getenv("LOCAL_TIMEZONE")
        if not local_tz:
            print(f"{C.b}{C.ivory}Warning: LOCAL_TIMEZONE not set, defaulting to 'UTC'.{C.r}")
            local_tz = "UTC"
        self._local_timezone: str = local_tz

        # Retrieve the 'TIMEZONE' variable; if it is not set, warn and default to "UTC".
        tz: Optional[str] = os.getenv("TIMEZONE")
        if not tz:
            # Print a warning message in colored text.
            print(f"{C.b}{C.ivory}Warning: TIMEZONE not set, defaulting to 'UTC'.{C.r}")
            tz = "UTC"
        self._timezone: str = tz

        # Aggregate the fetched environment variables into a dictionary for ease of access and dynamic printing.
        self._env_vars = {
            "ENV": self._env,
            "TIMEZONE": self._timezone,
            "LOCAL_TIMEZONE": self._local_timezone
        }

    def print_environment_info(self) -> None:
        """
        Print the environment variables in a structured, colored format.
        
        Process:
            - Define a separator line with a bright pink color for visual clarity.
            - Print a header message indicating the start of environment variable output.
            - Iterate through each key-value pair in the environment variables dictionary,
              printing each in a formatted manner with designated colors.
            - Print the separator line again to close the output.
        """
        # Create a separator line with bright pink coloring.
        dash_line = f"{C.b}{C.bright_pink}" + "-" * 41 + f"{C.r}"
        print(dash_line)
        print(f"{C.b}{C.ivory}Environment Variables:{C.r}")
        # Iterate over each key and value in the environment variables.
        for key, value in self._env_vars.items():
            # Print the key in bright pink and the value in ivory, with formatting.
            print(f"    - {C.b}{C.bright_pink}{key}{C.r} -> {C.b}{C.ivory}{value}{C.r}")
        # Print the bottom separator.
        print(dash_line)

    @property
    def environment(self) -> tuple:
        """
        Property: environment
        
        Returns:
            tuple: A tuple containing the ENV, TIMEZONE, and LOCAL_TIMEZONE values.
        """
        # Return a tuple of the key environment variables.
        return self._env, self._timezone, self._local_timezone

    @property
    def environment_vars(self) -> dict:
        """
        Property: environment_vars
        
        Returns:
            dict: A dictionary containing all the environment variables.
        """
        # Return the dictionary of environment variables.
        return self._env_vars