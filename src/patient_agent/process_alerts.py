from kafka import KafkaConsumer, KafkaProducer
import streamlit as st
import json
from monitor_patients import thresholds_map
from producer import tname_mapping

# # 🔹 Kafka Configuration
# KAFKA_BROKER = "localhost:9092"  # Update if needed
# ALERTS_TOPIC = "alerts_topic"

## 🔹 Initialize Kafka Producer
# producer = KafkaProducer(
#     bootstrap_servers=KAFKA_BROKER,
#     value_serializer=lambda v: json.dumps(v).encode('utf-8')
# )
# data_placeholder = st.empty()
# def process_alerts_by_id(data):
#     """Consume alerts from Kafka and return them as structured data."""
#     track = tname_mapping[data['tname']] 
#     row = thresholds_map[track] if track in thresholds_map else None
    
#     if row:
#         min_value = row['min']
#         max_value = row['max']
#         data_placeholder.empty()
#         # Check if value is out of range
#         if data['value'] < min_value or data['value'] > max_value:
#             # st.write("🚨 Alert: Out-of-range value detected!")
            
#             # 🔹 Prepare alert message
#             alert_message = {
#                 "caseid": data["caseid"],
#                 "tname": data["tname"],
#                 "value": data["value"],
#                 "min_value": min_value,
#                 "max_value": max_value,
#                 "unit": row.get("unit", "N/A"),
#                 "time": data["time"],
#                 "age":data["age"],
#                 "department": data.get("department", "N/A"),
#             }
            
#             # 🔹 Send alert to Kafka `alerts_topic`
#             producer.send(ALERTS_TOPIC, value=alert_message)
#             producer.flush()
#             st.write("✅ Alert sent to Kafka!")
#             data_placeholder.write(
#             f"✅ Alert sent to Kafka!"
#         )

#         else:
#             st.write("✅ Value is within range.")
#     else:
#         st.write(f"⚠️ No threshold found for track: {track}")
        
        
        
        
import pathway as pw
import streamlit as st
import json
import pandas as pd # Added pandas import
from monitor_patients import thresholds_map
from producer import tname_mapping

# 🔹 Kafka Configuration
KAFKA_BROKER = "localhost:9092"
ALERTS_TOPIC = "alerts_topic"
rdkafka_settings = {
    "bootstrap.servers": "localhost:9092",
    "group.id": "pathway_group",
    "auto.offset.reset": "latest",
}

# 🔹 Define Alert Schema
class AlertSchema(pw.Schema):
    caseid: str
    time: str
    department: str
    tracks: dict  # Contains all track data
    alerts: dict  # Contains only the tracks/signals that exceeded thresholds
    patient_info: dict  # Contains patient demographic information

def process_alerts_by_id(data):
    """Consume alerts from Kafka and process them using Pathway.
    Handles both individual track messages and grouped track messages.
    Sends the complete patient data along with flags for values that exceed thresholds.
    """
    # Initialize Kafka Producer
    producer_kafka = KafkaProducer(
        bootstrap_servers=KAFKA_BROKER,
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )
    
    # Initialize the alert message with the complete data
    alert_message = {
        "caseid": data.get("caseid", ""),
        "time": data.get("time", ""),
        "department": data.get("department", "N/A"),
        "tracks": {},  # Will contain all track data
        "alerts": {},  # Will contain only the tracks/signals that exceeded thresholds
        "patient_info": {
            "age": data.get("age", 0),
            "sex": data.get("sex", "Unknown"),
            "lab_results": data.get("lab_results", {})
        }
    }
    
    # Flag to track if any thresholds were exceeded
    thresholds_exceeded = False
    
    # Check if this is a grouped message (has 'tracks' field) or individual track message
    if "tracks" in data:
        # Process grouped message with multiple tracks
        alert_message["tracks"] = data["tracks"]  # Store all tracks data
        
        for track_id, track_info in data["tracks"].items():
            track_name = track_info["name"]
            
            # Process each signal in the track
            for signal_name, signal_value in track_info["signals"].items():
                # Check if we have thresholds for this signal
                if signal_name in thresholds_map:
                    row = thresholds_map[signal_name]
                    min_value = row["min"]
                    max_value = row["max"]
                    
                    # Check if value is out of range
                    if signal_value < min_value or signal_value > max_value:
                        thresholds_exceeded = True
                        st.write(f"🚨 Alert: Out-of-range value detected for {signal_name}!")
                        
                        # Add to alerts dictionary
                        if track_id not in alert_message["alerts"]:
                            alert_message["alerts"][track_id] = {
                                "name": track_name,
                                "signals": {}
                            }
                        
                        # Add the signal that exceeded thresholds
                        alert_message["alerts"][track_id]["signals"][signal_name] = {
                            "value": signal_value,
                            "min_value": min_value,
                            "max_value": max_value,
                            "unit": row.get("unit", "N/A"),
                            "exceeded": True
                        }
    else:
        # Handle legacy format (individual track message)
        if "tname" in data and "value" in data:
            track_name = data["tname"]
            signal_value = data["value"]
            
            # Add to tracks dictionary
            track_id = data.get("tid", "track_1")
            alert_message["tracks"][track_id] = {
                "name": track_name,
                "signals": {track_name: signal_value}
            }
            
            # Check if this track has thresholds
            if track_name in thresholds_map:
                row = thresholds_map[track_name]
            else:
                # Try to get the mapped name if it exists in tname_mapping
                mapped_name = tname_mapping.get(track_name, track_name)
                row = thresholds_map.get(mapped_name, None)
            
            if row:
                min_value = row["min"]
                max_value = row["max"]

                # Check if value is out of range
                if signal_value < min_value or signal_value > max_value:
                    thresholds_exceeded = True
                    st.write(f"🚨 Alert: Out-of-range value detected for {track_name}!")

                    # Add to alerts dictionary
                    alert_message["alerts"][track_id] = {
                        "name": track_name,
                        "signals": {
                            track_name: {
                                "value": signal_value,
                                "min_value": min_value,
                                "max_value": max_value,
                                "unit": row.get("unit", "N/A"),
                                "exceeded": True
                            }
                        }
                    }
            else:
                st.write(f"⚠️ No threshold found for track: {track_name}")
    
    # Send the complete alert message to Kafka if any thresholds were exceeded
    if thresholds_exceeded:
        producer_kafka.send(ALERTS_TOPIC, value=alert_message)
        producer_kafka.flush()
        st.write(f"✅ Alert sent to Kafka with complete patient data and threshold flags!")
    
    # Return the complete data for further processing if needed
    return alert_message


# 🔹 Wrapper function for Streamlit
def run_alert_processing(data):
    """Wrapper to call `process_alerts_by_id()` safely in Streamlit.
    Returns the processed data with alert flags.
    """
    return process_alerts_by_id(data)
