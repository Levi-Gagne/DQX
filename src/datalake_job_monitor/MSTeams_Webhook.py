# Databricks notebook source
# MAGIC %md
# MAGIC # Teams Webhook

# COMMAND ----------

# MAGIC %md
# MAGIC ## Documentation
# MAGIC This module processes failed job records and sends notifications to Microsoft Teams using Adaptive Cards. It leverages Apache Spark to query and update a Delta table containing job run data. Configuration is loaded from a YAML file, and two main classes handle the functionality: `TeamsNotifier` and `JobFailureProcessor`.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## TeamsNotifier Class
# MAGIC
# MAGIC ### Overview
# MAGIC The `TeamsNotifier` class is responsible for sending Adaptive Card notifications to Microsoft Teams. It loads a JSON template, populates it with dynamic content (such as a random GIF, timestamps, and job details), and posts the card via a webhook.
# MAGIC
# MAGIC ### Initialization
# MAGIC - **Method:** `__init__(self, webhook_url, local_timezone)`
# MAGIC - **Purpose:** Initializes the notifier with:
# MAGIC   - `webhook_url`: The Microsoft Teams webhook URL (sourced from YAML).
# MAGIC   - `local_timezone`: The local timezone for time formatting (also from YAML).
# MAGIC
# MAGIC ### Methods
# MAGIC
# MAGIC #### 1. `replace_placeholders`
# MAGIC - **Signature:** `@staticmethod def replace_placeholders(obj, placeholders)`
# MAGIC - **Purpose:** Recursively replaces placeholders in a JSON template.
# MAGIC - **Parameters:**
# MAGIC   - `obj`: A JSON object (can be a dict, list, or string) that may contain placeholders.
# MAGIC   - `placeholders`: A dictionary mapping placeholder keys to their replacement values.
# MAGIC - **Returns:** The JSON object with placeholders replaced by actual values.
# MAGIC
# MAGIC #### 2. `load_and_populate_card`
# MAGIC - **Signature:** `def load_and_populate_card(self, template_path, placeholders, team_emails)`
# MAGIC - **Purpose:** Loads an Adaptive Card JSON template from file, replaces placeholders, and populates Microsoft Teams mentions.
# MAGIC - **Parameters:**
# MAGIC   - `template_path`: Path to the JSON template file.
# MAGIC   - `placeholders`: A dictionary of keys corresponding to placeholders in the template.
# MAGIC   - `team_emails`: A mapping of team member names to their email addresses.
# MAGIC - **Process:**
# MAGIC   - Reads the JSON template.
# MAGIC   - Uses `replace_placeholders` to inject dynamic content.
# MAGIC   - Constructs a mentions list for the team members.
# MAGIC   - Injects the mentions into the Adaptive Card JSON structure.
# MAGIC - **Returns:** The populated Adaptive Card as a dictionary.
# MAGIC
# MAGIC #### 3. `send_notification`
# MAGIC - **Signature:** `def send_notification(self, job_details, team_emails, message_index=None, total_messages=None)`
# MAGIC - **Purpose:** Sends an Adaptive Card notification to Microsoft Teams.
# MAGIC - **Parameters:**
# MAGIC   - `job_details`: Dictionary containing job information (e.g., `job_name`, `job_id`, `run_id`, `run_page_url`, `start_time`, `duration`).
# MAGIC   - `team_emails`: A mapping of team member names to email addresses.
# MAGIC   - `message_index` (optional): The current message number for cases where multiple notifications are sent.
# MAGIC   - `total_messages` (optional): The total number of messages being sent.
# MAGIC - **Process:**
# MAGIC   - Fetches a random GIF URL using `Gif.get_random_gif()`. If the call fails, a default URL is used.
# MAGIC   - Formats the current datetime in the specified local timezone.
# MAGIC   - Constructs a greeting text, optionally including message numbering.
# MAGIC   - Defines placeholders to be replaced in the JSON template.
# MAGIC   - Calls `load_and_populate_card` to obtain the fully populated Adaptive Card.
# MAGIC   - Sends the Adaptive Card to Microsoft Teams via an HTTP POST request to the provided webhook URL.
# MAGIC   - Logs the response status to confirm whether the message was sent successfully.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## JobFailureProcessor Class
# MAGIC
# MAGIC ### Overview
# MAGIC The `JobFailureProcessor` class queries a target Delta table for failed jobs and triggers notifications via the `TeamsNotifier`. It also updates the records to mark that notifications have been sent.
# MAGIC
# MAGIC ### Initialization
# MAGIC - **Method:** `__init__(self, config_path: str)`
# MAGIC - **Purpose:** Sets up the processor by:
# MAGIC   - Loading configuration from a YAML file using `YamlConfig`.
# MAGIC   - Defining the target Delta table.
# MAGIC   - Initializing a Spark session.
# MAGIC   - Retrieving the Teams webhook URL and local timezone from the configuration.
# MAGIC   - Constructing a dictionary mapping team emails.
# MAGIC   - Instantiating the `TeamsNotifier`.
# MAGIC - **Parameter:**
# MAGIC   - `config_path`: Path to the YAML configuration file (e.g., `yaml/jobMonitor.yaml`).
# MAGIC
# MAGIC ### Methods
# MAGIC
# MAGIC #### `process_failures`
# MAGIC - **Signature:** `def process_failures(self) -> None`
# MAGIC - **Purpose:** Processes failed job records by:
# MAGIC   - Querying the target Delta table for jobs with a `FAILED` result and no prior notification (`msteams_notif` equals 0).
# MAGIC   - Filtering records within a lookback period specified in the YAML configuration.
# MAGIC   - Iterating over the retrieved records:
# MAGIC     - Formatting each record’s job details.
# MAGIC     - Sending a notification via `TeamsNotifier.send_notification` (with message numbering if applicable).
# MAGIC     - Executing an SQL update to mark the record as notified and update its modification timestamp.
# MAGIC   - Logging processing steps and handling any errors during notification or update operations.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Usage
# MAGIC
# MAGIC - **Execution:**
# MAGIC   - The module is designed to be run as a standalone script or within a notebook cell.
# MAGIC   - When executed, it performs the following steps:
# MAGIC     1. Loads the YAML configuration (e.g., `yaml/jobMonitor.yaml`).
# MAGIC     2. Initializes the `JobFailureProcessor`.
# MAGIC     3. Calls `process_failures()` to query for failed jobs, send notifications to Microsoft Teams, and update records in the Delta table.
# MAGIC
# MAGIC - **Dependencies:**
# MAGIC   - **Python Standard Libraries:** `json`, `requests`, `datetime`, `zoneinfo`
# MAGIC   - **PySpark:** `SparkSession` and SQL functions from `pyspark.sql`
# MAGIC   - **Custom Modules:**
# MAGIC     - `utils.yamlConfig`: For loading YAML configuration.
# MAGIC     - `utils.messageConfig`: For obtaining a random GIF URL.
# MAGIC
# MAGIC - **Entry Point:**
# MAGIC   ```python
# MAGIC     if __name__ == "__main__":
# MAGIC         CONFIG_PATH = "yaml/jobMonitor.yaml"  # Update this path as needed.
# MAGIC         processor = JobFailureProcessor(CONFIG_PATH)
# MAGIC         processor.process_failures()```

# COMMAND ----------

# DBTITLE 1,Execution Flow
"""
START: process_job_failures_and_send_notifications

|
|-- 1. Set environment context (`env`):
|   |
|   |-- 1a. Pass `env` to:
|   |   |-- Target Delta table (e.g., dq_{env}.monitoring.job_run_audit)
|   |   |-- Distribution list volume (e.g., /Volumes/dq_{env}/monitoring/distribution_lists)
|   |   |-- Teams webhook (from job-monitor.yaml; controls notification channel)
|   |

|-- 2. Query job audit table for failure records:
|   |
|   |-- 2a. Filter for records where:
|   |   |-- result_state == 'FAILED'
|   |   |-- msteams_notification == 0
|   |   |-- start_time >= current_timestamp() - interval <lookback_hours> hours
|   |   |
|   |   |-- 2a.i. If **no records** found:
|   |   |   |-- EXIT (no notifications to send)
|   |   |
|   |   |-- 2a.ii. If **failures found**:
|   |       |-- CONTINUE with all matching records
|   |

|-- 3. For each failed job record:
|   |
|   |-- 3a. Build notification recipient list:
|   |   |-- Start with static team emails from config (teams_emails in job-monitor.yaml)
|   |   |-- For each YAML file in the distribution list volume:
|   |   |   |-- i.   Get file stem (e.g., 'SchemaSquad' from 'SchemaSquad.yaml')
|   |   |   |-- ii.  If stem found as a substring in the tags column (case sensitive):
|   |   |       |-- Parse YAML
|   |   |       |-- Add `distribution` email (if present)
|   |   |       |-- Add all `members` emails
|   |   |-- Deduplicate (dict keys)
|   |
|   |-- 3b. Send Adaptive Card notification to Teams:
|   |   |-- Use the full recipient list for Teams mentions and message text
|   |   |-- Card fields populated with job and notification context
|   |
|   |-- 3c. Mark record as notified:
|       |-- UPDATE job_run_audit row (where job_id/run_id/start_date match)
|           |-- SET: msteams_notification = 1
|           |--      updated_at = current timestamp
|           |--      updated_by = 'AdminUser' (or process user)
|

|-- 4. Print summary:
|   |-- For each notified record, print job name and run ID
|   |-- END process

END: process_job_failures_and_send_notifications
"""

# COMMAND ----------

# MAGIC %md
# MAGIC ## Code

# COMMAND ----------

# DBTITLE 1,Imports
# Standard library imports
import os
import json
import yaml
import requests
from datetime import datetime
from zoneinfo import ZoneInfo

# PySpark modules for Spark session creation and DataFrame operations.
from pyspark.sql import SparkSession 
from pyspark.sql.functions import col, expr

# Custom utility modules.
from utils.colorConfig import C
from utils.gifConfig import GifUtils
from utils.yamlConfig import YamlConfig
from utils.environmentConfig import EnvironmentConfig
from utils.emailConfig import email_to_name

# COMMAND ----------

# DBTITLE 1,TeamsNotifier
class TeamsNotifier:
    def __init__(self, webhook_url: str, processing_timezone: str, template_path: str):
        self.webhook_url = webhook_url
        self.processing_timezone = processing_timezone
        self.template_path = template_path
        self.recipient_lists_path = None

    @staticmethod
    def replace_placeholders(obj, placeholders):
        if isinstance(obj, dict):
            return {k: TeamsNotifier.replace_placeholders(v, placeholders) for k, v in obj.items()}
        elif isinstance(obj, list):
            return [TeamsNotifier.replace_placeholders(item, placeholders) for item in obj]
        elif isinstance(obj, str):
            template_str = obj.replace("{{", "{").replace("}}", "}")
            try:
                return template_str.format(**placeholders)
            except KeyError:
                return template_str
        else:
            return obj

    @staticmethod
    def format_duration(raw_duration) -> str:
        try:
            if isinstance(raw_duration, str) and raw_duration.lower().endswith(" seconds"):
                raw_duration = raw_duration.lower().replace(" seconds", "").strip()
            total_seconds = int(float(raw_duration))
        except Exception:
            return raw_duration
        hrs = total_seconds // 3600
        remainder = total_seconds % 3600
        mins = remainder // 60
        secs = remainder % 60
        parts = []
        if hrs > 0:
            parts.append(f"{hrs}hrs")
        if mins > 0:
            parts.append(f"{mins}mins")
        if secs > 0 or (hrs == 0 and mins == 0):
            parts.append(f"{secs}secs")
        return " ".join(parts)

    def send_notification(self, job_details: dict, team_emails: dict, extra_emails: list = None,
                         message_index: int = None, total_messages: int = None, matched_tags: list = None):
        try:
            gif_url = GifUtils.get_random_gif()
        except Exception as e:
            print(f"{C.b}{C.red}Warning: Could not fetch GIF image: {e}{C.b}")
            gif_url = "https://example.com/dummy.gif"

        current_datetime = datetime.now(ZoneInfo(self.processing_timezone)).strftime("%Y-%m-%d | %I:%M %p")

        # TEAM NAMES: Always from team_emails (job-monitor.yaml), sorted
        team_names = sorted([email_to_name(e) for e in team_emails.keys()])
        mentions_text = " ".join(team_names)

        # TAG LINES: Each tag gets its own line, blank line after team names if tags exist
        tag_line_list = []
        if extra_emails and matched_tags:
            tag_set = set(extra_emails)
            for base_file_name in matched_tags:
                yaml_path = os.path.join(self.recipient_lists_path, f"{base_file_name}.yaml")
                with open(yaml_path, "r") as f:
                    data = yaml.safe_load(f)
                tag_emails = []
                if base_file_name in data and "members" in data[base_file_name]:
                    tag_emails = data[base_file_name]["members"]
                elif "members" in data:
                    tag_emails = data["members"]
                tag_names = sorted([email_to_name(e) for e in tag_emails if e in tag_set])
                if tag_names:
                    tag_line_list.append(f"Tag: {base_file_name} - {' '.join(tag_names)}")
        if tag_line_list:
            mentions_text = f"{mentions_text}\n\n" + "\n".join(tag_line_list)

        # MENTIONS ENTITIES (actual Teams tagging): combine all emails for @
        combined_emails = set(team_emails.keys())
        if extra_emails:
            combined_emails.update(extra_emails)
        mentions_entities = [
            {
                "type": "mention",
                "text": f"<at>{email}</at>",
                "mentioned": {"id": email, "name": email}
            }
            for email in combined_emails
        ]

        greeting_text = "Message  -  "
        if message_index is not None and total_messages is not None:
            greeting_text += f"{message_index}/{total_messages}"
        raw_duration = job_details.get("duration", "N/A")
        formatted_duration = TeamsNotifier.format_duration(raw_duration) if raw_duration != "N/A" else "N/A"

        placeholders = {
            "gif_url": gif_url,
            "current_datetime": current_datetime,
            "greeting_text": greeting_text,
            "job_name": job_details.get("job_name", "N/A"),
            "job_id": job_details.get("job_id", "N/A"),
            "run_id": job_details.get("run_id", "N/A"),
            "start_time": job_details.get("start_time", "N/A"),
            "duration": formatted_duration,
            "run_page_url": job_details.get("run_page_url", "#"),
            "mentions_text": mentions_text
        }

        with open(self.template_path, "r") as f:
            card_template = json.load(f)
        populated_card = TeamsNotifier.replace_placeholders(card_template, placeholders)
        populated_card["attachments"][0]["content"]["msteams"]["entities"] = mentions_entities

        response = requests.post(
            self.webhook_url,
            data=json.dumps(populated_card),
            headers={'Content-Type': 'application/json'}
        )
        if response.status_code in (200, 202):
            print(
                f"{C.b}{C.ivory}Notification accepted by flow for job "
                f"'{C.r}{C.b}{C.pale_turquoise}{job_details.get('job_name', 'N/A')}{C.r}{C.b}{C.ivory}', "
                f"run ID '{C.r}{C.b}{C.bubblegum_pink}{job_details.get('run_id', 'N/A')}{C.r}'.{C.b}"
            )
        else:
            print(
                f"{C.red}Failed to send notification for job "
                f"'{job_details.get('job_name', 'N/A')}', run ID "
                f"'{job_details.get('run_id', 'N/A')}'. Status code: {response.status_code}.{C.b}"
            )
        return response

# COMMAND ----------

# DBTITLE 1,JobFailureProcessor
# ----------------------------------------------------------------------
# JobFailureProcessor: Processes job failure records and sends Teams notifications.
# ----------------------------------------------------------------------
class JobFailureProcessor:
    def __init__(self, config_path: str, template_path: str, env: str, processing_timezone: str):
        print(f"{C.aqua_blue}===== INITIALIZING JOB FAILURE PROCESSOR ====={C.b}")

        self.spark = SparkSession.builder.getOrCreate()

        self.cfg = YamlConfig(config_path, env=env, spark=self.spark)

        self.target_table = self.cfg.full_table_name
        print(f"{C.ivory}Target table: {C.bubblegum_pink}{self.target_table}{C.ivory}.{C.b}")

        self.lookback_hours = int(self.cfg.config["teams"]["lookback_date"].split()[0])
        print(f"{C.ivory}Lookback hours: {C.bubblegum_pink}{self.lookback_hours}{C.ivory}.{C.b}")

        webhook_secrets = self.cfg.config["teams"]["webhook_secrets"][env]
        webhook_url = self.cfg.dbutils.secrets.get(
            scope=webhook_secrets["webhook_scope"],
            key=webhook_secrets["webhook_key"]
        )
        print(f"{C.ivory}Retrieved webhook URL: {C.bubblegum_pink}{webhook_url}{C.ivory}.{C.b}")

        self.processing_timezone = processing_timezone
        print(f"{C.ivory}Processing timezone: {C.bubblegum_pink}{self.processing_timezone}{C.ivory}.{C.b}")
        
        self.team_emails = {email: email for email in self.cfg.config["teams"]["teams_emails"]}

        raw_path = self.cfg.config["teams"]["recipients_list_path"]
        self.recipient_lists_path = raw_path.format(env=env)
        print(f"{C.ivory}Recipient List Path: {C.bubblegum_pink}{self.recipient_lists_path}{C.ivory}.{C.b}")

        self.notifier = TeamsNotifier(webhook_url, self.processing_timezone, template_path)

        self.notifier.recipient_lists_path = self.recipient_lists_path

    def extract_tag_matches(self, row_tags, distribution_list_names):
        if not row_tags:
            return []
        return [base_file_name for base_file_name in distribution_list_names if base_file_name in row_tags]

    def get_distribution_list_names(self):
        return [
            os.path.splitext(f)[0]
            for f in os.listdir(self.recipient_lists_path)
            if f.endswith(".yaml")
        ]

    def load_distribution_list_members(self, base_file_name):
        yaml_path = os.path.join(self.recipient_lists_path, f"{base_file_name}.yaml")
        with open(yaml_path, "r") as f:
            data = yaml.safe_load(f)
        if base_file_name in data and "members" in data[base_file_name]:
            return data[base_file_name]["members"]
        elif "members" in data:
            return data["members"]
        return []

    def process_failures(self) -> None:
        print(f"{C.aqua_blue}===== STARTING FAILURE PROCESSING ====={C.b}")
        df = self.spark.table(self.target_table).filter(
            (col("result_state") == "FAILED") &
            (col("msteams_notification") == 0) &
            (col("start_time") >= expr(f"current_timestamp() - interval {self.lookback_hours} hours"))
        )
        records = df.collect()
        total_messages = len(records)
        print(f"{C.ivory}Found {C.bubblegum_pink}{total_messages}{C.ivory} record(s) matching conditions.{C.b}")
        dist_lists = self.get_distribution_list_names()
        updated_records = []
        for idx, record in enumerate(records, start=1):
            job = {
                "job_name": record.job_name or "N/A",
                "job_id": str(record.job_id) if record.job_id else "N/A",
                "run_id": str(record.run_id) if record.run_id else "N/A",
                "start_time": record.start_time.strftime("%Y-%m-%d | %I:%M %p") if record.start_time else "N/A",
                "duration": record.duration or "N/A",
                "run_page_url": record.run_page_url or "#"
            }
            print(f"{C.ivory}Processing job '{C.bubblegum_pink}{job['job_name']}{C.ivory}' with run ID '{C.bubblegum_pink}{job['run_id']}{C.ivory}'.{C.b}")
            row_tags = getattr(record, "tags", "")
            matched_tags = self.extract_tag_matches(row_tags, dist_lists)
            extra_emails = []
            for base_file_name in matched_tags:
                extra_emails.extend(self.load_distribution_list_members(base_file_name))
            try:
                self.notifier.send_notification(
                    job,
                    self.team_emails,
                    extra_emails=extra_emails,
                    message_index=idx,
                    total_messages=total_messages,
                    matched_tags=matched_tags
                )
                utc_write_str = datetime.now(ZoneInfo(self.processing_timezone)).strftime("%Y-%m-%d %H:%M:%S")
                update_query = f"""
                    UPDATE {self.target_table}
                    SET msteams_notification = 1,
                        updated_at = timestamp'{utc_write_str}',
                        updated_by = 'AdminUser'
                    WHERE job_id = '{job['job_id']}'
                      AND run_id = '{job['run_id']}'
                      AND start_date = '{record.start_date}'
                """
                self.spark.sql(update_query)
                updated_records.append((job["job_name"], job["run_id"]))
            except Exception as e:
                print(f"{C.red}Error processing job_id {job['job_id']}: {e}{C.b}")
        print(f"\n{C.aqua_blue}===== FAILURE PROCESSING COMPLETED ====={C.b}")
        print(f"{C.b}{C.ivory}Updated{C.r}{C.b} {C.electric_purple}{len(updated_records)}{C.r}{C.b}{C.ivory} record(s).{C.r}")
        for job_name, run_id in updated_records:
            print(f"{C.b}{C.ivory}Job:{C.r}{C.b} {C.bubblegum_pink}'{job_name}'{C.r}{C.b}{C.ivory}, Run ID:{C.r}{C.b} {C.lavender}{run_id}{C.b}")
        print(f"{C.aqua_blue}===== END SUMMARY ====={C.b}\n")

# COMMAND ----------

# DBTITLE 1,Main Function
if __name__ == "__main__":
    CONFIG_PATH   = "yaml/job-monitor.yaml"
    TEMPLATE_PATH = "json/msteams_job_failure_notification.json"

    env, processing_timezone, local_timezone = EnvironmentConfig().environment

    print(f"{C.ivory}Running in ENV={C.bubblegum_pink}{env}{C.ivory}, "
          f"processing TZ={C.bubblegum_pink}{processing_timezone}{C.ivory}.{C.b}")

    try:
        processor = JobFailureProcessor(CONFIG_PATH, TEMPLATE_PATH, env=env, processing_timezone=processing_timezone)
        processor.process_failures()
    except Exception as e:
        print(f"{C.red}An error occurred during processing: {e}{C.b}")
        raise
