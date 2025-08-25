import os
from dotenv import load_dotenv
from groq import Groq
import json
import requests
from datetime import datetime
from zoneinfo import ZoneInfo
import dateparser
import sqlite3
import pandas as pd
from datetime import datetime
from logger_config import logger


load_dotenv()

# === Configuration ===
Groq_api_key = os.getenv("GROQ_API_KEY")
client = Groq(api_key=Groq_api_key)

def summarize_conversation_transcript(conversation_transcript):
    system_prompt = """You are a helpful assistant that extracts summary and tasks from AI call transcripts. 
    The conversation transcript is provided by the user.
    The details of the tasks should be detailed and descriptive. 
    Make sure to include all tasks mentioned in the conversation transcript by the user or customer. 
    Make sure you **do not** miss any tasks and/or their details.
    from the transcript, you have to summarize the conversation in much detail.
    from the transcript, you have to extract the tasks that the customer wants to perform.
    from the transcript, you have to check if the customer wants to schedule a meeting with the owner.
    if the customer wants to schedule a meeting, you have to return the meeting status as True.
    if the customer wants to schedule an in-person meeting, you have to return meeting_type_in_person in JSON response with a boolean value True. If the customer does not want to schedule an in-person meeting, you have to return meeting_type_in_person as False.
    "In-person meeting" means the customer wants to visit a showroom or a store or wants to have a meeting at a physical location.
    if the customer wants to schedule an in person meeting, then find out the time that the user prefers for the in-person meeting and return it in the JSON response as meeting_time_in_person_raw.
    if the customer wants to schedule a virtual meeting, you have to return meeting_type_virtual in JSON response with a boolean value True. If the customer does not want to schedule a virtual meeting, you have to return meeting_type_virtual as False.
    if the customer wants to schedule a virtual meeting, then find out the time that the user prefers for the virtual meeting and return it in the JSON response as meeting_time_virtual_raw.
    if the customer wants to schedule both a virtual and an in-person meeting, you have to return both meeting_type_in_person and meeting_type_virtual as True. In this case, you have to find out the time that the user prefers for both meetings and return it in the JSON response as meeting_time_in_person_raw and meeting_time_virtual_raw.
    if the customer does not want to schedule a meeting, you have to return the meeting status as False and also return meeting_type_in_person and meeting_type_virtual as False. In this case, you do not need to return meeting_time_in_person_raw and meeting_time_virtual_raw. Just return them as empty strings. But it is applicable only if the customer does not want to schedule a meeting.
    if meeting_type_in_person is True, then meeting_time_in_person_raw should not be empty.
    if meeting_type_virtual is True, then meeting_time_virtual_raw should not be empty.
    If meeting_type_in_person is False, then meeting_time_in_person_raw should be an empty string.
    If meeting_type_virtual is False, then meeting_time_virtual_raw should be an empty string.
    If both meeting_type_in_person and meeting_type_virtual are False, then both meeting_time_in_person_raw and meeting_time_virtual_raw should be empty strings.
    If both meeting_type_in_person and meeting_type_virtual are True, then both meeting_time_in_person_raw and meeting_time_virtual_raw should contain the respective preferred times.
    your final JSON response should look like this (this is just an example, do not use these values.):\n\n
    {
        "summary": "the detailed summary of the conversation",
        "tasks": 1. Get the product details , 2. Call Tom , ...,
        "meeting_schedule_is_true": true/false,
        "meeting_type_in_person": true/false,
        "meeting_type_virtual": true/false,
        "meeting_time_in_person_raw": "in person raw meeting time as string",
        "meeting_time_virtual_raw": "virtual meeting time as string"
    }
    Make sure to return the JSON response in a single line without any extra spaces or newlines.
    Do not return any other text or explanation. Just return the JSON response as it is.
    Do not return ````json`` or any other formatting. Just return the JSON response as it is."""
    
    messages = [
        {
            "role": "system",
            "content": system_prompt
        },
        {
            "role": "user",
            "content": f"""Here is the conversation transcript:\n\n
                f"{conversation_transcript}\n\n
                "Please extract any summary,tasks, meeting_schedule_is_true, meeting_time and meeting_type mentioned by the customer in this conversation transcript.
                "Only return valid JSON.
                            """
        }
    ]

    try:
        logger.info("[summarize_conversation_transcript] Starting summarization of conversation transcript.\n\n")
        response = client.chat.completions.create(
            model="llama-3.3-70b-versatile",
            messages=messages,
            # response_format="json"  # Optional: helps nudge some models into structured output
        )

        logger.info("[summarize_conversation_transcript] Received response from LLM.\n\n")
        content = response.choices[0].message.content.strip()
        logger.info(f"[summarize_conversation_transcript] Raw content from LLM: {content}\n\n")
        # Ensure the content is a valid JSON string
        if not content.startswith('{') or not content.endswith('}'):
            logger.error("[summarize_conversation_transcript] Response content is not a valid JSON object.\n\n")
            raise ValueError("Response content is not a valid JSON object.")
        # Fix common JSON formatting issues if needed
        content = content.replace("\n", "").replace("\t", "")
        content = content.replace("True", "true").replace("False", "false")

        parsed = json.loads(content)
        logger.info(f"[summarize_conversation_transcript] Parsed JSON (final output): {parsed}\n\n")
        return parsed

    except json.JSONDecodeError as e:
        logger.error(f"[summarize_conversation_transcript] JSON decoding failed: {e}\n\n")
        return {}
    except Exception as e:
        logger.error(f"[summarize_conversation_transcript] Unexpected error: {e}\n\n")
        return {}


def update_customer_data_notes_and_tasks(call_id, parsed, db_path="queue.db"):
    """
    Appends new notes and tasks (with timestamp) to the existing notes and tasks columns for a given call_id in customer_data.
    """
    if not parsed:
        logger.warning(f"[update_customer_data_notes_and_tasks] No valid parsed data available for call_id: {call_id}\n\n")
        
        conn = sqlite3.connect(db_path)
        c = conn.cursor()
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        c.execute("SELECT notes, tasks FROM customer_data WHERE call_id = ?", (call_id,))
        row = c.fetchone()
        if row:
            existing_notes, existing_tasks = row
            new_notes = "No summary available. Conversation transcript missing."
            new_tasks = "No tasks found for this call."
            updated_notes = (existing_notes or "") + f"\n[{timestamp}] " + (new_notes or "")
            updated_tasks = (existing_tasks or "") + f"\n[{timestamp}] " + (new_tasks or "")
            c.execute("UPDATE customer_data SET notes = ?, tasks = ? WHERE call_id = ?", (updated_notes, updated_tasks, call_id))
            conn.commit()
        conn.close()

    if parsed:
        conn = sqlite3.connect(db_path)
        c = conn.cursor()
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        c.execute("SELECT notes, tasks FROM customer_data WHERE call_id = ?", (call_id,))
        row = c.fetchone()
        if row:
            existing_notes, existing_tasks = row
            new_notes = parsed.get("summary", "No summary available.")
            new_tasks = parsed.get("tasks", "No tasks found for this call.")
            updated_notes = (existing_notes or "") + f"\n[{timestamp}] " + (new_notes or "")
            updated_tasks = (existing_tasks or "") + f"\n[{timestamp}] " + (new_tasks or "")
            c.execute("UPDATE customer_data SET notes = ?, tasks = ? WHERE call_id = ?", (updated_notes, updated_tasks, call_id))
            conn.commit()
        conn.close()

def export_customer_data_to_excel(db_path="queue.db", excel_path="resultant_excel.xlsx"):
    """
    Exports the entire customer_data table to resultant_excel.xlsx.
    Use this after all calls are processed to get the final Excel.
    """
    conn = sqlite3.connect(db_path)
    df = pd.read_sql_query("SELECT * FROM customer_data", conn)
    df.to_excel(excel_path, index=False)
    # Clear the customer_data table after export
    conn.execute("DELETE FROM customer_data")
    conn.commit()
    conn.close()


def send_meeting_invite(parsed, customer_name, customer_email):
    """
    Sends a meeting invite based on the parsed data.
    """
    meeting_status = parsed.get("meeting_schedule_is_true", False)
    logger.info(f"[log_data] Meeting status: {meeting_status}\n\n")
    meeting_status_in_person = parsed.get("meeting_type_in_person", False)
    logger.info(f"[log_data] In-person meeting status: {meeting_status_in_person}\n\n")
    meeting_status_virtual = parsed.get("meeting_type_virtual", False)
    logger.info(f"[log_data] Virtual meeting status: {meeting_status_virtual}\n\n")
    meeting_time_in_person_raw = parsed.get("meeting_time_in_person_raw", "None")
    logger.info(f"[log_data] Raw in-person meeting time: {meeting_time_in_person_raw}\n\n")
    meeting_time_virtual_raw = parsed.get("meeting_time_virtual_raw", "None")
    logger.info(f"[log_data] Raw virtual meeting time: {meeting_time_virtual_raw}\n\n")

    if meeting_status is True:
        logger.info(f"[log_data] Meeting status - In-person: {meeting_status_in_person}, Virtual: {meeting_status_virtual}\n\n")

        # Handle in-person meeting invite
        if meeting_status_in_person is True:
            try:
                raw_meeting_time_in_person = parsed.get("meeting_time_in_person_raw", "")
                logger.info(f"[log_data] Raw in-person meeting time: {raw_meeting_time_in_person}\n\n")
                current_datetime_est_in_person = datetime.now(ZoneInfo("America/New_York"))
                logger.info(f"[log_data] Current datetime in EST for in-person meeting: {current_datetime_est_in_person}\n\n Now getting true meeting time from dateparser.")
                meeting_time_in_person_est = dateparser.parse(
                    raw_meeting_time_in_person,
                    settings={
                        'RELATIVE_BASE': current_datetime_est_in_person,
                        'TIMEZONE': 'America/New_York',
                        'RETURN_AS_TIMEZONE_AWARE': True
                    }
                )
                logger.info(f"[log_data] Parsed in-person meeting time (Dateparser response): {meeting_time_in_person_est}\n\n")
                if meeting_time_in_person_est is not None:
                    meeting_time_in_person = meeting_time_in_person_est.isoformat()
                    logger.info(f"[log_data] In-person meeting time parsed (final meeting time being sent to meeting_invite API in ISO format): {meeting_time_in_person}\n\n")
                    # Sending the in-person meeting invite
                    logger.info(f"[log_data] In-person meeting time parsed: {meeting_time_in_person}\n\n")
                    response = requests.post(
                        headers={"Content-Type": "application/json"},
                        url="https://func-send-calendar-invite-tm-dev-fhdcbce9ebdpcmcg.eastus-01.azurewebsites.net/api/schedule_meeting",
                        json={
                            "attendee_email": customer_email,
                            "attendee_name": customer_name,
                            "subject": "In-person Meeting invite from Architessa",
                            "body": """Hi, we're really grateful that you could give us some of your precious time.
                                        We'll make sure it's worth your while.""",
                            "start_time": meeting_time_in_person,
                            "duration_minutes": 30,
                            "meeting_type": "in_person"
                        }
                    )
                    response.raise_for_status()  # Raise an error for bad responses
                    logger.info("[log_data] In-person calendar invite sent successfully.\n\n")
                else:
                    logger.error(f"[send_meeting_invite] Could not parse in-person meeting time: '{raw_meeting_time_in_person}'")
            except Exception as e:
                logger.error(f"[send_meeting_invite] Error sending in-person meeting invite: {e}")

        # Handle virtual meeting invite
        if meeting_status_virtual is True:
            try:
                raw_meeting_time_virtual = parsed.get("meeting_time_virtual_raw", "No virtual meeting time provided")
                logger.info(f"[log_data] Raw virtual meeting time: {raw_meeting_time_virtual}\n\n")
                current_datetime_est_virtual = datetime.now(ZoneInfo("America/New_York"))
                logger.info(f"[log_data] Current datetime in EST for virtual meeting: {current_datetime_est_virtual}\n\n")
                meeting_time_virtual_est = dateparser.parse(
                    raw_meeting_time_virtual,
                    settings={
                        'RELATIVE_BASE': current_datetime_est_virtual,
                        'TIMEZONE': 'America/New_York',
                        'RETURN_AS_TIMEZONE_AWARE': True
                    }
                )
                logger.info(f"[log_data] Parsed virtual meeting time (Dateparser response): {meeting_time_virtual_est}\n\n")
                if meeting_time_virtual_est is not None:
                    meeting_time_virtual = meeting_time_virtual_est.isoformat()
                    logger.info(f"[log_data] Virtual meeting time parsed (final meeting time being sent to meeting_invite API in ISO format): {meeting_time_virtual}\n\n")
                    response = requests.post(
                        headers={"Content-Type": "application/json"},
                        url="https://func-send-calendar-invite-tm-dev-fhdcbce9ebdpcmcg.eastus-01.azurewebsites.net/api/schedule_meeting",
                        json={
                            "attendee_email": customer_email,
                            "attendee_name": customer_name,
                            "subject": "Virtual Meeting invite from Architessa",
                            "body": """Hi, we're really grateful that you could give us some of your precious time.
                                        We'll make sure it's worth your while.""",
                            "start_time": meeting_time_virtual,
                            "duration_minutes": 30,
                            "meeting_type": "virtual"
                        }
                    )
                    response.raise_for_status()  # Raise an error for bad responses
                    logger.info("[log_data] Virtual calendar invite sent successfully.\n\n")
                else:
                    logger.error(f"[send_meeting_invite] Could not parse virtual meeting time: '{raw_meeting_time_virtual}'")
            except Exception as e:
                logger.error(f"[send_meeting_invite] Error sending virtual meeting invite: {e}")

        else:
            logger.info("[log_data] No meeting scheduled.\n\n")
            meeting_time_in_person = ""
            meeting_time_virtual = ""

        logger.info("[log_data] logger call to Salesforce.\n\n")