from datetime import timedelta
import json
import os
from dotenv import load_dotenv
import google.generativeai as genai
from langchain_core.prompts import ChatPromptTemplate
from langchain_core.tools import Tool
from langchain_core.language_models import BaseChatModel
from langchain_core.messages import HumanMessage, SystemMessage, AIMessage
from langchain_core.outputs import ChatResult, ChatGeneration
from typing import Any
import pathway as pw
from tenacity import retry, retry_if_exception_type, stop_after_attempt, wait_exponential
import time

# ------------------- Load Environment Variables -------------------
load_dotenv()

# ------------------- Gemini API Configuration -------------------
api_key = os.getenv("GEMINI_API_KEY")
if not api_key:
    raise ValueError("GEMINI_API_KEY not found. Check your .env file!")
genai.configure(api_key=api_key)

# ------------------- Custom Gemini LLM Wrapper -------------------
class GeminiChatModel(BaseChatModel):
    def _generate(self, messages, stop=None, **kwargs):
        """Generate response using Gemini model."""
        combined_message = "\n".join(
            f"{msg.type.capitalize()}: {msg.content}" if hasattr(msg, "content") else "" for msg in messages
        )
        model = genai.GenerativeModel("gemini-2.0-flash")
        response = model.generate_content(combined_message)
        return ChatResult(
            generations=[
                ChatGeneration(
                    message=AIMessage(content=response.text),
                    text=response.text,
                )
            ]
        )
    def bind_tools(self, tools):
        return self
    def _llm_type(self):
        return "gemini"

# ✅ Initialize Gemini LLM
llm = GeminiChatModel()

# ------------------- Kafka Configuration -------------------
rdkafka_settings = {
    "bootstrap.servers": "localhost:9092",
    "group.id": "pathway_group",
    "auto.offset.reset": "latest",
}

# Define the schema for incoming Kafka messages
class AlertSchema(pw.Schema):
    caseid: str
    time: float  # Changed from str to float to work with windowby
    department: str
    tracks: dict  # Contains all track data
    alerts: dict  # Contains only the tracks/signals that exceeded thresholds
    patient_info: dict  # Contains patient demographic information

# Read messages from the Kafka topic
alerts_stream = pw.io.kafka.read(
    rdkafka_settings=rdkafka_settings,
    topic="alerts_topic",
    schema=AlertSchema,
    format="json",
)

# Optional: Print the raw schema for debugging
print("Raw alerts schema:", alerts_stream.schema)

# ------------------- Deduplicate Alerts -------------------
# Here we use a tumbling window of 60 seconds and group by caseid,
# taking only the first alert within each window for each case.
deduped_alerts = alerts_stream.windowby(
    alerts_stream.time,
    window=pw.temporal.tumbling(duration=1),
    instance=alerts_stream.caseid
).reduce(
    caseid     = pw.reducers.earliest(pw.this.caseid),
    time       = pw.reducers.earliest(pw.this.time),
    department = pw.reducers.earliest(pw.this.department),
    tracks     = pw.reducers.earliest(pw.this.tracks),
    alerts     = pw.reducers.earliest(pw.this.alerts),
    patient_info = pw.reducers.earliest(pw.this.patient_info)
)




# ------------------- Define Function to Print and Query Gemini -------------------
# This function prints the alert and sends it to Gemini.
def print_message(caseid: str, time_val: float, department: str, tracks: dict, alerts: dict, patient_info: dict) -> str:
    # Convert JSON objects to Python dicts
    patient_info_dict = patient_info.as_dict()
    alerts_dict = alerts.as_dict()
    tracks_dict = tracks.as_dict()

    # Extract patient information
    age = patient_info_dict.get('age', 'Unknown')
    sex = patient_info_dict.get('sex', 'Unknown')
    
    # Build message with alert information
    message = f"Received alert - Case ID: {caseid}, Time: {time_val:.2f}, Department: {department}\n"
    message += f"Patient: Age {age}, Sex: {sex}\n"
    
    # Add information about the alerts (tracks that exceeded thresholds)
    if alerts_dict:
        message += "\nAbnormal Vital Signs:\n"
        for track_id, track_data in alerts_dict.items():
            track_data_dict = track_data if isinstance(track_data, dict) else track_data.as_dict()
            track_name = track_data_dict.get('name', 'Unknown')
            message += f"\nTrack: {track_name} (ID: {track_id})\n"
            
            signals_dict = track_data_dict.get('signals', {})
            signals_dict = signals_dict if isinstance(signals_dict, dict) else signals_dict.as_dict()

            for signal_name, signal_data in signals_dict.items():
                signal_data_dict = signal_data if isinstance(signal_data, dict) else signal_data.as_dict()
                value = signal_data_dict.get('value', 'Unknown')
                min_value = signal_data_dict.get('min_value', 'Unknown')
                max_value = signal_data_dict.get('max_value', 'Unknown')
                unit = signal_data_dict.get('unit', '')
                
                message += f"  - {signal_name}: {value} {unit} (Expected Range: {min_value}-{max_value} {unit})\n"
    else:
        message += "\nNo abnormal vital signs detected."
    
    print(message)
    
    # Prepare a prompt for Gemini
    prompt = f"""
    You are a medical assistant. Analyze the following alert:
    {message}
    
    Provide a concise summary and recommendations based on the patient's age, sex, and the abnormal vital signs.
    """
    response = llm.invoke([HumanMessage(content=prompt)])
    
    # Print Gemini's response
    print(f"🤖 Gemini response for Case {caseid}: {response}")
    
    # Sleep to prevent rapid API calls
    time.sleep(5)
    
    return message

# ------------------- Apply the Function to Deduplicated Alerts -------------------
final_alerts = deduped_alerts.select(
    debug_output = pw.apply(
        print_message,
        deduped_alerts.caseid,
        deduped_alerts.time,
        deduped_alerts.department,
        deduped_alerts.tracks,
        deduped_alerts.alerts,
        deduped_alerts.patient_info
    )
)

# Materialize the table and print the results (for debugging)
pw.debug.compute_and_print(final_alerts)

# ------------------- Run the Pathway Computation -------------------
# Use run_forever to continuously monitor alerts.
pw.run()
