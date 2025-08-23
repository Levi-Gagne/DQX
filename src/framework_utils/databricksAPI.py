# ================================================================================
# Module: utils/databricksAPIConfig.py
# Description:
#   This module provides the DatabricksAPIClient class, which abstracts the interactions
#   with the Databricks REST API endpoints. It handles authentication, URL construction,
#   and communication with the API. The client supports retrieving run details, fetching
#   all runs since a given cutoff timestamp (with pagination), and obtaining job details.
#
#   Workflow:
#   1. Initialization:
#      - Strips any protocol (http/https) from the provided databricks_instance.
#      - For testing, replaces dummy instances with localhost.
#      - Validates that the API version is present in the configuration.
#      - Constructs the base URL for API calls.
#      - Loads and validates required endpoints (e.g., job_runs_list, job_run_details).
#      - Sets up authentication headers using the provided token.
#
#   2. API Methods:
#      - get_run_details: Retrieves detailed information for a specific run.
#      - get_all_runs_since: Iteratively fetches runs from the API since a specified cutoff,
#        handling pagination and filtering out older runs (unless still running).
#      - get_job_details: Retrieves job details for a given job ID.
#
# Usage:
#      - Instantiate DatabricksAPIClient with the databricks_instance, token, and api_config.
#      - Call the provided methods to interact with the Databricks API.
#
# Note:
#      - Ensure that the YAML configuration contains the required API version and endpoint definitions.
#
# Author: Levi Gagne
# Created Date: 2025-03-25
# Last Modified: 2025-04-16
# ================================================================================

import os
import sys
import yaml
import requests
from datetime import datetime, timezone
from zoneinfo import ZoneInfo
from typing import Dict, Any, List, Optional

class DatabricksAPIClient:
    """
    Class: DatabricksAPIClient
    Description:
      A client for interacting with Databricks REST API endpoints. This client handles:
         - Authentication via bearer token.
         - Construction of the base URL using the databricks_instance and API version.
         - Loading and verification of required API endpoints from the YAML configuration.
         - Handling of API communication for retrieving run details, fetching runs since a cutoff timestamp,
           and obtaining job details.
    """
    def __init__(self, databricks_instance: str, token: str, api_config: Dict[str, Any]) -> None:
        """
        Initialize the DatabricksAPIClient instance.
        
        Parameters:
            databricks_instance (str): The instance URL or identifier for the Databricks service.
            token (str): Authentication bearer token.
            api_config (Dict[str, Any]): YAML configuration dictionary containing API version and endpoint definitions.
            
        Process:
            - Remove any HTTP/HTTPS protocol from the databricks_instance.
            - If the instance starts with "dummy_", replace it with "localhost:8080" for testing.
            - Save the cleaned databricks_instance, token, and api_config.
            - Validate that the API version is provided; if not, raise an Exception.
            - Construct the base URL for API calls.
            - Load API endpoints from api_config and verify that required endpoints exist.
            - Set up authentication headers using the provided token.
        """
        # Remove 'https://' prefix if present.
        if databricks_instance.startswith("https://"):
            databricks_instance = databricks_instance[len("https://"):]
        # Remove 'http://' prefix if present.
        elif databricks_instance.startswith("http://"):
            databricks_instance = databricks_instance[len("http://"):]
        
        # For testing: if instance starts with "dummy_", substitute with 'localhost:8080'
        if databricks_instance.startswith("dummy_"):
            print("Using dummy databricks_instance, replacing with 'localhost:8080' for testing.")
            databricks_instance = "localhost:8080"
        
        # Remove any trailing '/' to ensure consistent URL formation.
        self.databricks_instance: str = databricks_instance.rstrip("/")
        # Store the authentication token.
        self.token: str = token
        # Save the API configuration dictionary.
        self.api_config: Dict[str, Any] = api_config

        # Validate that the API version is provided in the configuration.
        api_version = self.api_config.get("version")
        if not api_version:
            raise Exception("API version must be specified in the YAML configuration under the 'api' section.")
        # Construct the base URL using the cleaned instance and the API version.
        self.base_url: str = f"https://{self.databricks_instance}/api/{api_version}"
        
        # Load API endpoints from the configuration.
        self.endpoints: Dict[str, Any] = self.api_config.get("endpoints", {})
        # Verify that the required endpoint for job runs list is provided.
        if "job_runs_list" not in self.endpoints:
            raise Exception("Missing 'job_runs_list' endpoint in the YAML configuration.")
        # Verify that the required endpoint for job run details is provided.
        if "job_run_details" not in self.endpoints:
            raise Exception("Missing 'job_run_details' endpoint in the YAML configuration.")
        
        # Set up the headers for authentication; using bearer token.
        self.headers: Dict[str, str] = {"Authorization": f"Bearer {self.token}"}

    def get_run_details(self, run_id: str) -> Dict[str, Any]:
        """
        Retrieve details of a specific run.
        
        Parameters:
            run_id (str): The identifier of the run for which details are requested.
            
        Returns:
            Dict[str, Any]: A dictionary containing the JSON response with run details.
            
        Process:
            - Retrieve the 'job_run_details' endpoint from the endpoints configuration.
            - Construct the complete URL by combining the base URL with the endpoint.
            - Set up the parameters dictionary with the run_id.
            - Send a GET request to the URL with headers and parameters.
            - Raise an error if the response status is not OK.
            - Return the JSON response.
        """
        # Get the endpoint for run details from the configuration.
        endpoint: str = self.endpoints["job_run_details"]
        # Construct the full URL for the run details API call.
        url: str = f"{self.base_url}{endpoint}"
        # Prepare query parameters with the run_id.
        params: Dict[str, Any] = {"run_id": run_id}
        # Execute the GET request to the API.
        response = requests.get(url, headers=self.headers, params=params)
        # Check for HTTP errors; will raise an exception if found.
        response.raise_for_status()
        # Return the response data in JSON format.
        return response.json()

    def get_all_runs_since(self, cutoff: int) -> List[Dict[str, Any]]:
        """
        Fetch all job runs from the API since a given cutoff timestamp.
        
        Parameters:
            cutoff (int): The cutoff timestamp (in milliseconds or seconds, as defined) to filter runs.
            
        Returns:
            List[Dict[str, Any]]: A list of dictionaries, each containing run details that meet the cutoff criteria.
            
        Process:
            - Print start information to indicate API fetching has started.
            - Initialize an empty list to collect valid runs.
            - Use a loop to handle API pagination with page tokens.
            - For each page:
                * Construct the URL using the 'job_runs_list' endpoint.
                * If a page token exists, add it to the request parameters.
                * Send a GET request; raise an error if unsuccessful.
                * Parse the JSON response and iterate through the runs.
                * For each run, check its start_time:
                    - If the run's start_time is before the cutoff and the run is not in the RUNNING state, skip it.
                    - Otherwise, retrieve full run details using get_run_details and append to the runs_list.
                * Update the next_page_token from the response.
                * Break the loop if no further pages exist.
            - Print finishing information and the total number of fetched runs.
            - Return the aggregated runs_list.
        """
        # Print header for API run fetching.
        print("===== FETCHING RUNS FROM API =====")
        # Initialize an empty list to collect runs.
        runs_list: List[Dict[str, Any]] = []
        # Initialize next_page_token as None for pagination.
        next_page_token: Optional[str] = None
        
        # Loop to handle multiple pages of API response.
        while True:
            # Retrieve the endpoint for listing job runs.
            endpoint: str = self.endpoints["job_runs_list"]
            # Construct the full URL for the job runs list API call.
            url: str = f"{self.base_url}{endpoint}"
            # Initialize parameters; will be updated if pagination token is available.
            params: Dict[str, Any] = {}
            # If a page token exists, include it in the parameters.
            if next_page_token:
                params["page_token"] = next_page_token
            # Send the GET request to fetch the list of runs.
            response = requests.get(url, headers=self.headers, params=params)
            # Check for HTTP errors.
            response.raise_for_status()
            # Parse the JSON response.
            data = response.json()
            # Extract the list of runs from the response.
            runs = data.get("runs", [])
            # Iterate through each run in the response.
            for run in runs:
                # Extract the start time of the run.
                start_time = run.get("start_time")
                # Skip runs with start_time before the cutoff unless the run is still in progress.
                if cutoff and start_time and start_time < cutoff and run.get("state", {}).get("life_cycle_state") != "RUNNING":
                    continue
                # Convert run_id to string to ensure consistency in API calls.
                run_id = str(run.get("run_id"))
                try:
                    # Retrieve full details for the run using its run_id.
                    full_run = self.get_run_details(run_id)
                    # Append the full run details to the list.
                    runs_list.append(full_run)
                except Exception as e:
                    # Log any errors encountered during retrieval of run details.
                    print(f"Error processing run_id {run_id}: {e}")
            # Update next_page_token for pagination from the JSON response.
            next_page_token = data.get("next_page_token")
            # If no further page token is found, exit the loop.
            if not next_page_token:
                break
        
        # Print summary of the API call and total runs fetched.
        print(f"Finished API call: Fetched {len(runs_list)} runs since cutoff.")
        print("===== END FETCHING RUNS =====\n")
        # Return the aggregated list of runs.
        return runs_list

    def get_job_details(self, job_id: str) -> Dict[str, Any]:
        """
        Retrieve job details for a given job ID.
        
        Parameters:
            job_id (str): The identifier of the job to fetch details for.
            
        Returns:
            Dict[str, Any]: A dictionary containing the job details in JSON format.
            
        Process:
            - Retrieve the 'job_details' endpoint from the configuration.
            - Raise an exception if the endpoint is not defined.
            - Construct the URL using the base URL and the job_details endpoint.
            - Prepare query parameters using the provided job_id.
            - Send a GET request to the API.
            - Raise an exception if the request fails.
            - Return the JSON response.
        """
        # Get the 'job_details' endpoint; if missing, raise an error.
        endpoint: str = self.endpoints.get("job_details")
        if not endpoint:
            raise Exception("Missing 'job_details' endpoint in YAML configuration.")
        # Construct the full URL for the job details API call.
        url: str = f"{self.base_url}{endpoint}"
        # Prepare request parameters with the job_id.
        params: Dict[str, Any] = {"job_id": job_id}
        # Execute the GET request.
        response = requests.get(url, headers=self.headers, params=params)
        # Check for HTTP errors.
        response.raise_for_status()
        # Return the JSON response containing the job details.
        return response.json()