import requests
import pandas as pd
import io
import time
import json
import random
from kafka import KafkaProducer
import streamlit as st
from process_alerts import process_alerts_by_id
from case_tracking import run_specific_case_pipeline
# 🔹 Kafka Producer Setup
producer_kafka = KafkaProducer(
    bootstrap_servers="localhost:9092",
    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
)

# 🔹 Load track list & cases data
import os
import pandas as pd

# Get the current script directory
BASE_DIR = os.path.dirname(os.path.abspath(__file__))

# Construct absolute paths
track_list_path = os.path.join(BASE_DIR, "track_list.xlsx")
cases_df_path = os.path.join(BASE_DIR, "cases.xlsx")
labs_df_path = os.path.join(BASE_DIR, "vitaldb_labs.xlsx")

# Read Excel files
track_list = pd.read_excel(track_list_path)
cases_df = pd.read_excel(cases_df_path)
labs_df = pd.read_excel(labs_df_path)


# 🔹 Signal Name Mapping (tname_mapping)
tname_mapping = {
    "SNUADC/ART": "Arterial Pressure Wave",
    "SNUADC/CVP": "Central Venous Pressure Wave",
    "SNUADC/ECG_II": "ECG Lead II Wave",
    "SNUADC/ECG_V5": "ECG Lead V5 Wave",
    "SNUADC/FEM": "Femoral Arterial Pressure Wave",
    "SNUADC/PLETH": "Plethysmography Wave",
    "Solar8000/ART_DBP": "Diastolic Arterial Pressure",
    "Solar8000/ART_MBP": "Mean Arterial Pressure",
    "Solar8000/ART_SBP": "Systolic Arterial Pressure",
    "Solar8000/BT": "Body Temperature",
    "Solar8000/CVP": "Central Venous Pressure",
    "Solar8000/ETCO2": "End-Tidal CO2",
    "Solar8000/FEM_DBP": "Femoral Diastolic Arterial Pressure",
    "Solar8000/FEM_MBP": "Femoral Mean Arterial Pressure",
    "Solar8000/FEM_SBP": "Femoral Systolic Arterial Pressure",
    "Solar8000/FEO2": "Fraction of Expired O2",
    "Solar8000/FIO2": "Fraction of Inspired O2",
    "Solar8000/GAS2_EXPIRED": "Expiratory Volatile Concentration",
    "Solar8000/GAS2_INSPIRED": "Inspiratory Volatile Concentration",
    "Solar8000/HR": "Heart Rate",
    "Solar8000/INCO2": "Inspiratory CO2",
    "Solar8000/NIBP_DBP": "Non-Invasive Diastolic Arterial Pressure",
    "Solar8000/NIBP_MBP": "Non-Invasive Mean Arterial Pressure",
    "Solar8000/NIBP_SBP": "Non-Invasive Systolic Arterial Pressure",
    "Solar8000/PA_DBP": "Pulmonary Diastolic Arterial Pressure",
    "Solar8000/PA_MBP": "Pulmonary Mean Arterial Pressure",
    "Solar8000/PA_SBP": "Pulmonary Systolic Arterial Pressure",
    "Solar8000/PLETH_HR": "Heart Rate (Plethysmography)",
    "Solar8000/PLETH_SPO2": "Percutaneous Oxygen Saturation",
    "Solar8000/RR": "Respiratory Rate (ECG)",
    "Solar8000/RR_CO2": "Respiratory Rate (Capnography)",
    "Solar8000/ST_AVF": "ST Segment (aVF)",
    "Solar8000/ST_AVL": "ST Segment (aVL)",
    "Solar8000/ST_AVR": "ST Segment (aVR)",
    "Solar8000/ST_I": "ST Segment (I)",
    "Solar8000/ST_II": "ST Segment (II)",
    "Solar8000/ST_III": "ST Segment (III)",
    "Solar8000/ST_V5": "ST Segment (V5)",
    "Solar8000/VENT_COMPL": "Airway Compliance (Ventilator)",
    "Solar8000/VENT_INSP_TM": "Inspiratory Time (Ventilator)",
    "Solar8000/VENT_MAWP": "Mean Airway Pressure (Ventilator)",
    "Solar8000/VENT_MEAS_PEEP": "Positive End-Expiratory Pressure (Ventilator)",
    "Solar8000/VENT_MV": "Minute Ventilation (Ventilator)",
    "Solar8000/VENT_PIP": "Peak Inspiratory Pressure (Ventilator)",
    "Solar8000/VENT_PPLAT": "Plateau Pressure (Ventilator)",
    "Solar8000/VENT_RR": "Respiratory Rate (Ventilator)",
    "Solar8000/VENT_SET_FIO2": "Set Fraction of Inspired O2 (Ventilator)",
    "Solar8000/VENT_SET_PCP": "Set Peak Inspiratory Pressure (Ventilator)",
    "Solar8000/VENT_SET_TV": "Set Tidal Volume (Ventilator)",
    "Solar8000/VENT_TV": "Measured Tidal Volume (Ventilator)",
    "Primus/AWP": "Airway Pressure Wave",
    "Primus/CO2": "Capnography Wave",
    "Primus/COMPLIANCE": "Airway Compliance",
    "Primus/ETCO2": "End-Tidal CO2",
    "Primus/EXP_DES": "Expiratory Desflurane Pressure",
    "Primus/EXP_SEVO": "Expiratory Sevoflurane Pressure",
    "Primus/FEN2O": "Fraction of Expired N2O",
    "Primus/FEO2": "Fraction of Expired O2",
    "Primus/FIN2O": "Fraction of Inspired N2O",
    "Primus/FIO2": "Fraction of Inspired O2",
    "Primus/FLOW_AIR": "Flow Rate of Air",
    "Primus/FLOW_N2O": "Flow Rate of N2O",
    "Primus/FLOW_O2": "Flow Rate of O2",
    "Primus/INCO2": "Inspiratory CO2",
    "Primus/INSP_DES": "Inspiratory Desflurane Pressure",
    "Primus/INSP_SEVO": "Inspiratory Sevoflurane Pressure",
    "Primus/MAC": "Minimum Alveolar Concentration",
    "Primus/MAWP_MBAR": "Mean Airway Pressure",
    "Primus/MV": "Minute Volume",
    "Primus/PAMB_MBAR": "Ambient Pressure",
    "Primus/PEEP_MBAR": "Positive End-Expiratory Pressure",
    "Primus/PIP_MBAR": "Peak Inspiratory Pressure",
    "Primus/PPLAT_MBAR": "Plateau Pressure",
    "Primus/RR_CO2": "Respiratory Rate (Capnography)",
    "Primus/SET_AGE": "Patient Age",
    "Primus/SET_FIO2": "Set Fraction of Inspired O2",
    "Primus/SET_FLOW_TRIG": "Set Flow Trigger Value",
    "Primus/SET_FRESH_FLOW": "Set Fresh Gas Flow",
    "Primus/SET_INSP_PAUSE": "Set Inspiratory Pause",
    "Primus/SET_INSP_PRES": "Set Inspiratory Pressure",
    "Primus/SET_INSP_TM": "Set Inspiratory Time",
    "Primus/SET_INTER_PEEP": "Set Positive End-Expiratory Pressure",
    "Primus/SET_PIP": "Set Peak Inspiratory Pressure",
    "Primus/SET_RR_IPPV": "Set Respiratory Rate",
    "Primus/SET_TV_L": "Set Tidal Volume (Liters)",
    "Primus/TV": "Tidal Volume",
    "Primus/VENT_LEAK": "Ventilator Leakage",
    "Orchestra/AMD_RATE": "Infusion Rate (Amiodarone)",
    "Orchestra/AMD_VOL": "Infused Volume (Amiodarone)",
    "Orchestra/DEX2_RATE": "Infusion Rate (Dexmedetomidine 2 mcg/mL)",
    "Orchestra/DEX2_VOL": "Infused Volume (Dexmedetomidine 2 mcg/mL)",
    "Orchestra/DEX4_RATE": "Infusion Rate (Dexmedetomidine 4 mcg/mL)",
    "Orchestra/DEX4_VOL": "Infused Volume (Dexmedetomidine 4 mcg/mL)",
    "Orchestra/DOBU_RATE": "Infusion Rate (Dobutamine)",
    "Orchestra/DOBU_VOL": "Infused Volume (Dobutamine)",
    "Orchestra/DOPA_RATE": "Infusion Rate (Dopamine)",
    "Orchestra/DOPA_VOL": "Infused Volume (Dopamine)",
    "Orchestra/DTZ_RATE": "Infusion Rate (Diltiazem)",
    "Orchestra/DTZ_VOL": "Infused Volume (Diltiazem)",
    "Orchestra/EPI_RATE": "Infusion Rate (Epinephrine)",
    "Orchestra/EPI_VOL": "Infused Volume (Epinephrine)",
    "Orchestra/FUT_RATE": "Infusion Rate (Futhan)",
    "Orchestra/FUT_VOL": "Infused Volume (Futhan)",
    "Orchestra/MRN_RATE": "Infusion Rate (Milrinone)",
    "Orchestra/MRN_VOL": "Infused Volume (Milrinone)",
    "Orchestra/NEPI_RATE": "Infusion Rate (Norepinephrine)",
    "Orchestra/NEPI_VOL": "Infused Volume (Norepinephrine)",
    "Orchestra/NPS_RATE": "Infusion Rate (Nitroprusside)",
    "Orchestra/NPS_VOL": "Infused Volume (Nitroprusside)",
    "Orchestra/NTG_RATE": "Infusion Rate (Nitroglycerin)",
    "Orchestra/NTG_VOL": "Infused Volume (Nitroglycerin)",
    "Orchestra/OXY_RATE": "Infusion Rate (Oxytocin)",
    "Orchestra/OXY_VOL": "Infused Volume (Oxytocin)",
    "Orchestra/PGE1_RATE": "Infusion Rate (Prostaglandin-E1)",
    "Orchestra/PGE1_VOL": "Infused Volume (Prostaglandin-E1)",
    "Orchestra/PHEN_RATE": "Infusion Rate (Phenylephrine)",
    "Orchestra/PHEN_VOL": "Infused Volume (Phenylephrine)",
    "Orchestra/PPF20_CE": "Effect-Site Concentration (Propofol)",
    "Orchestra/PPF20_CP": "Plasma Concentration (Propofol)",
    "Orchestra/PPF20_CT": "Target Concentration (Propofol)",
    "Orchestra/PPF20_RATE": "Infusion Rate (Propofol)",
    "Orchestra/PPF20_VOL": "Infused Volume (Propofol)",
    "Orchestra/RFTN20_CE": "Effect-Site Concentration (Remifentanil 20 mcg/mL)",
    "Orchestra/RFTN20_CP": "Plasma Concentration (Remifentanil 20 mcg/mL)",
    "Orchestra/RFTN20_CT": "Target Concentration (Remifentanil 20 mcg/mL)",
    "Orchestra/RFTN20_RATE": "Infusion Rate (Remifentanil 20 mcg/mL)",
    "Orchestra/RFTN20_VOL": "Infused Volume (Remifentanil 20 mcg/mL)",
    "Orchestra/RFTN50_CE": "Effect-Site Concentration (Remifentanil 50 mcg/mL)",
    "Orchestra/RFTN50_CP": "Plasma Concentration (Remifentanil 50 mcg/mL)",
    "Orchestra/RFTN50_CT": "Target Concentration (Remifentanil 50 mcg/mL)",
    "Orchestra/RFTN50_RATE": "Infusion Rate (Remifentanil 50 mcg/mL)",
    "Orchestra/RFTN50_VOL": "Infused Volume (Remifentanil 50 mcg/mL)",
    "Orchestra/ROC_RATE": "Infusion Rate (Rocuronium)",
    "Orchestra/ROC_VOL": "Infused Volume (Rocuronium)",
    "Orchestra/VASO_RATE": "Infusion Rate (Vasopressin)",
    "Orchestra/VASO_VOL": "Infused Volume (Vasopressin)",
    "Orchestra/VEC_RATE": "Infusion Rate (Vecuronium)",
    "Orchestra/VEC_VOL": "Infused Volume (Vecuronium)",
    "BIS/BIS": "Bispectral Index",
    "BIS/EEG1_WAV": "EEG Wave (Channel 1)",
    "BIS/EEG2_WAV": "EEG Wave (Channel 2)",
    "BIS/EMG": "Electromyography Power",
    "BIS/SEF": "Spectral Edge Frequency",
    "BIS/SQI": "Signal Quality Index",
    "BIS/SR": "Suppression Ratio",
    "BIS/TOTPOW": "Total Power",
    "Invos/SCO2_L": "Cerebral Oxygen Saturation (Left)",
    "Invos/SCO2_R": "Cerebral Oxygen Saturation (Right)",
    "Vigileo/CI": "Cardiac Index",
    "Vigileo/CO": "Cardiac Output",
    "Vigileo/SV": "Stroke Volume",
    "Vigileo/SVI": "Stroke Volume Index",
    "Vigileo/SVV": "Stroke Volume Variation",
    "EV1000/ART_MBP": "Mean Arterial Pressure",
    "EV1000/CI": "Cardiac Index",
    "EV1000/CO": "Cardiac Output",
    "EV1000/CVP": "Central Venous Pressure Wave",
    "EV1000/SV": "Stroke Volume",
    "EV1000/SVI": "Stroke Volume Index",
    "EV1000/SVR": "Systemic Vascular Resistance",
    "EV1000/SVRI": "Systemic Vascular Resistance Index",
    "EV1000/SVV": "Stroke Volume Variation",
    "Vigilance/BT_PA": "Pulmonary Artery Temperature",
    "Vigilance/CI": "Cardiac Index",
    "Vigilance/CO": "Cardiac Output",
    "Vigilance/EDV": "End-Diastolic Volume",
    "Vigilance/EDVI": "End-Diastolic Volume Index",
    "Vigilance/ESV": "End-Systolic Volume",
    "Vigilance/ESVI": "End-Systolic Volume Index",
    "Vigilance/HR_AVG": "Average Heart Rate",
    "Vigilance/RVEF": "Right Ventricular Ejection Fraction",
    "Vigilance/SNR": "Signal-to-Noise Ratio",
    "Vigilance/SQI": "Signal Quality Index",
    "Vigilance/SV": "Stroke Volume",
    "Vigilance/SVI": "Stroke Volume Index",
    "Vigilance/SVO2": "Mixed Venous Oxygen Saturation",
    "CardioQ/ABP": "Arterial Pressure Wave",
    "CardioQ/FLOW": "Flow Wave",
    "CardioQ/CI": "Cardiac Index",
    "CardioQ/CO": "Cardiac Output",
    "CardioQ/FTc": "Flow Time Corrected",
    "CardioQ/FTp": "Flow Time to Peak",
    "CardioQ/HR": "Heart Rate",
    "CardioQ/MA": "Mean Acceleration",
    "CardioQ/MD": "Minute Distance",
    "CardioQ/PV": "Peak Velocity",
    "CardioQ/SD": "Stroke Distance",
    "CardioQ/SV": "Stroke Volume",
    "CardioQ/SVI": "Stroke Volume Index",
    "FMS/FLOW_RATE": "Flow Rate",
    "FMS/INPUT_AMB_TEMP": "Input Ambient Temperature",
    "FMS/INPUT_TEMP": "Input Fluid Temperature",
    "FMS/OUTPUT_AMB_TEMP": "Output Ambient Temperature",
    "FMS/OUTPUT_TEMP": "Output Fluid Temperature",
    "FMS/PRESSURE": "Infusion Line Pressure",
    "FMS/TOTAL_VOL": "Total Infused Volume",
}

lab_results_map = {
    "wbc": "White Blood Cell Count",
    "hb": "Hemoglobin",
    "hct": "Hematocrit",
    "plt": "Platelet Count",
    "esr": "Erythrocyte Sedimentation Rate",
    "gluc": "Glucose",
    "tprot": "Total Protein",
    "alb": "Albumin",
    "tbil": "Total Bilirubin",
    "ast": "Aspartate Transferase",
    "alt": "Alanine Transferase",
    "bun": "Blood Urea Nitrogen",
    "cr": "Creatinine",
    "gfr": "Glomerular Filtration Rate",
    "ccr": "Creatinine Clearance",
    "na": "Sodium",
    "k": "Potassium",
    "ica": "Ionized Calcium",
    "cl": "Chloride",
    "ammo": "Ammonia",
    "crp": "C-Reactive Protein",
    "lac": "Lactate",
    "ptinr": "Prothrombin Time (INR)",
    "pt%": "Prothrombin Time (%)",
    "ptsec": "Prothrombin Time (sec)",
    "aptt": "Activated Partial Thromboplastin Time",
    "fib": "Fibrinogen",
    "ph": "pH",
    "pco2": "Partial Pressure of CO2",
    "po2": "Partial Pressure of O2",
    "hco3": "Bicarbonate",
    "be": "Base Excess",
    "sao2": "Arterial Oxygen Saturation",
}

# 🔹 Function to Fetch & Send One Track Data at a Time
# def fetch_track_data(caseid):
#     """Fetch all tracks for a given Case ID and send to Kafka."""
    
#     # **Get All Track IDs for Given Case ID**
#     print(f"🔍 Searching tracks for Case ID: {caseid}")
#     track_entries = track_list[track_list["caseid"] == int(caseid)]
#     print(" tracks found = ", len(track_entries))
#     time.sleep(3)
#     if track_entries.empty:
#         print(f"❌ No tracks found for Case ID {caseid}")
#         return

#     # **Iterate Over All Matching Tracks**
#     for _, track_entry in track_entries.iterrows():
#         track_id = track_entry["tid"]
#         print(f"Processing Track ID: {track_id}")
#         time.sleep(3)

#         # **🔹 API Fetch with Retry Logic**
#         max_retries, retry_delay = 3, 5
#         track_data = None

#         for attempt in range(max_retries):
#             response = requests.get(f"https://api.vitaldb.net/{track_id}", timeout=10)
#             if response.status_code == 200 and response.content:
#                 track_data = pd.read_csv(io.StringIO(response.content.decode('utf-8')))
#                 break
#             else:
#                 print(f"⚠️ Retry {attempt + 1}/{max_retries} for {track_id} failed. Retrying in {retry_delay}s...")
#                 time.sleep(retry_delay)

#         if track_data is None or track_data.empty:
#             print(f"❌ No valid data found for {track_id}. Skipping...")
#             continue
        
#         print(f"✅ Fetched Data for {track_id} - {track_data.shape[0]} rows")

#         # **Filter Invalid/Missing Values**
#         track_data = track_data.dropna()
#         track_data = track_data[track_data.iloc[:, 0] > 0]  # Remove time=0 rows
#         track_data = track_data[track_data.iloc[:, 1] > 0]  # Remove value=0 rows

#         if track_data.empty:
#             print(f"⚠️ No valid rows after filtering for {track_id}. Skipping...")
#             continue

#         # **Fetch Patient Details**
#         patient_info = cases_df[cases_df["caseid"] == int(caseid)].to_dict(orient="records")
#         patient_details = patient_info[0] if patient_info else {}

#         # **Fetch Lab Results**
#         lab_results = labs_df[labs_df["caseid"] == int(caseid)].to_dict(orient="records")
#         mapped_lab_results = {lab_results_map.get(lab["name"], lab["name"]): lab["result"] for lab in lab_results}

#         # **Send All Rows to Kafka**
#         for _, row in track_data.iterrows():
#             time_value = float(row.iloc[0]) if not pd.isna(row.iloc[0]) else 0.0

#             for signal_name in track_data.columns[1:]:
#                 signal_value = float(row[signal_name]) if not pd.isna(row[signal_name]) else 0.0

#                 # **Map signal name to user-friendly name**
#                 mapped_signal_name = tname_mapping.get(signal_name, signal_name)

#                 # **Prepare Kafka Message**
#                 message = {
#                     "caseid": caseid,
#                     "tname": mapped_signal_name,
#                     "tid": track_id,
#                     "time": time_value,
#                     "value": signal_value,
#                     "age": int(patient_details.get("age", 0)) if not pd.isna(patient_details.get("age", 0)) else 0,
#                     "sex": str(patient_details.get("sex", "Unknown")),
#                     "department": str(patient_details.get("department", "Unknown")),
#                     "lab_results": mapped_lab_results,
#                 }

#                 # **Send Message to Kafka**
#                 producer.send("case_tracking", message)
#                 print("message = ",message)
#                 time.sleep(5)

#         print(f"📤 Sent All Data for Track ID {track_id} to Kafka\n")
#         time.sleep(3)

def fetch_track_list(caseid):
    """Fetch available tracks for a given Case ID."""
    track_entries = track_list[track_list["caseid"] == int(caseid)]
    if track_entries.empty:
        st.warning(f"❌ No tracks found for Case ID {caseid}")
        return None
    return track_entries

# ✅ Fetch and Display Track Options
def display_track_options(track_entries):
    """Display track names with friendly names and allow user to choose."""
    track_options = {}
    print("\nTrack IDs found:")
    for idx, (_, track_entry) in enumerate(track_entries.iterrows(), start=1):
        track_id = track_entry["tid"]
        track_name = track_entry["tname"]
        friendly_name = tname_mapping.get(track_name, f"Unknown ({track_name})")
        track_options[idx] = (track_id, friendly_name)
        print(f"  {idx}. {track_id} - {friendly_name}")
    return track_options

# ✅ Fetch and Process Track Data
def fetch_track_data(caseid):
    """Fetch, process, and display real-time updates for all tracks, grouping data by timestamp."""
    st.sidebar.info(f"📡 Fetching all tracks for Case ID: {caseid}...")
    
    # Get all track entries for the given caseid
    case_tracks = track_list[track_list['caseid'] == int(caseid)]

    if case_tracks.empty:
        st.error(f"❌ No tracks found for Case ID {caseid}. Skipping...")
        return

    # **Fetch Patient Details once for the case**
    patient_info = cases_df[cases_df["caseid"] == int(caseid)].to_dict(orient="records")
    patient_details = patient_info[0] if patient_info else {}

    # **Fetch Lab Results once for the case**
    lab_results = labs_df[labs_df["caseid"] == int(caseid)].to_dict(orient="records")
    mapped_lab_results = {lab_results_map.get(lab["name"], lab["name"]): lab["result"] for lab in lab_results}

    # Dictionary to store all track data, keyed by track_id
    all_tracks_data = {}
    
    # First, fetch data for all tracks
    for index, track_row in case_tracks.iterrows():
        track_id = track_row['tid']
        track_name = track_row['tname']

        st.sidebar.info(f"Processing Track ID: {track_id} ({track_name})...")
        status_placeholder = st.empty() # Placeholder for individual track status

        max_retries, retry_delay = 3, 5
        track_data_df = None

        for attempt in range(max_retries):
            try:
                response = requests.get(f"https://api.vitaldb.net/{track_id}", timeout=10)
                if response.status_code == 200:
                    track_data_df = pd.read_csv(io.StringIO(response.content.decode('utf-8')))
                    print(f"✅ Data fetched successfully for {track_name} ({track_id})")
                    
                    break
                else:
                    status_placeholder.warning(f"⚠️ Attempt {attempt + 1}/{max_retries} for {track_name} ({track_id}) failed. Retrying...")
                    time.sleep(retry_delay)
            except Exception as e:
                status_placeholder.error(f"❌ Error fetching {track_name} ({track_id}): {str(e)}. Retrying in {retry_delay}s...")
                time.sleep(retry_delay)
        
        if track_data_df is None:
            status_placeholder.error(f"❌ No valid data found for {track_name} ({track_id}) after retries. Skipping...")
            continue # Move to the next track

        # ✅ Clean and Filter Track Data
        track_data_df = track_data_df.dropna()
        if not track_data_df.empty and track_data_df.shape[1] > 0:
             track_data_df = track_data_df[track_data_df.iloc[:, 0] > 0]  # Remove rows with time=0
        if not track_data_df.empty and track_data_df.shape[1] > 1:
            track_data_df = track_data_df[track_data_df.iloc[:, 1] > 0]  # Remove rows with value=0

        if track_data_df.empty:
            # st.error(f"⚠️ No valid rows after filtering for {track_name} ({track_id}). Skipping...")
            continue # Move to the next track
            
        # st.success(f"✅ Processing Data for {track_name} ({track_id}) - {track_data_df.shape[0]} rows")
        
        # Store the track data with its metadata
        all_tracks_data[track_id] = {
            'data': track_data_df,
            'name': track_name
        }
    
    if not all_tracks_data:
        st.error("❌ No valid track data found for any track. Exiting...")
        return
        
    # Create a dictionary to group measurements by timestamp
    # Structure: {timestamp: {track_id: {signal_name: value, ...}, ...}, ...}
    grouped_by_time = {}
    
    # Process all tracks and group by timestamp
    for track_id, track_info in all_tracks_data.items():
        track_data_df = track_info['data']
        track_name = track_info['name']
        
        for _, row in track_data_df.iterrows():
            time_value = float(row.iloc[0]) if not pd.isna(row.iloc[0]) else 0.0
            
            # Initialize the timestamp entry if it doesn't exist
            if time_value not in grouped_by_time:
                grouped_by_time[time_value] = {}
            
            # Initialize the track entry for this timestamp if it doesn't exist
            if track_id not in grouped_by_time[time_value]:
                grouped_by_time[time_value][track_id] = {
                    'track_name': track_name,
                    'signals': {}
                }
            
            # Add all signal values for this track at this timestamp
            for signal_idx, signal_name in enumerate(track_data_df.columns[1:], 1):
                if not pd.isna(row[signal_name]):
                    signal_value = float(row[signal_name])
                    mapped_signal_name = tname_mapping.get(signal_name, signal_name)
                    grouped_by_time[time_value][track_id]['signals'][mapped_signal_name] = signal_value
    
    # Prepare data for visualization
    # Create a dictionary to store time series data for each signal
    time_series_data = {}
    
    # Sort timestamps for ordered processing
    sorted_timestamps = sorted(grouped_by_time.keys())
    
    # Create containers for visualization and real-time data
    chart_container = st.container()
    data_placeholder = st.empty()
    
    # Initialize data structures for the chart
    for time_value in sorted_timestamps:
        for track_id, track_data in grouped_by_time[time_value].items():
            for signal_name, signal_value in track_data['signals'].items():
                # Create a unique identifier for each signal
                signal_key = f"{track_data['track_name']} - {signal_name}"
                
                # Initialize the time series if it doesn't exist
                if signal_key not in time_series_data:
                    time_series_data[signal_key] = {
                        'times': [],
                        'values': [],
                        'track_id': track_id
                    }
                
                # Add the data point
                time_series_data[signal_key]['times'].append(time_value)
                time_series_data[signal_key]['values'].append(signal_value)
    
    # Create a multi-line chart for visualization
    with chart_container:
        st.subheader(f"📊 Patient Vital Signs Over Time - Case ID: {caseid}")
        
        # Group signals by track for better organization
        tracks_to_plot = {}
        for signal_key, signal_data in time_series_data.items():
            track_id = signal_data['track_id']
            if track_id not in tracks_to_plot:
                tracks_to_plot[track_id] = []
            tracks_to_plot[track_id].append(signal_key)
        
        # Create tabs for different track categories
        tabs = st.tabs([f"Track {i+1}: {all_tracks_data[track_id]['name']}" for i, track_id in enumerate(tracks_to_plot.keys())])
        
        # Plot each track's signals in its own tab
        for i, (track_id, signal_keys) in enumerate(tracks_to_plot.items()):
            with tabs[i]:
                # Create a DataFrame for this track's signals
                track_df = pd.DataFrame()
                track_df['Time'] = sorted_timestamps
                
                # Add each signal as a column
                for signal_key in signal_keys:
                    # Create a full time series aligned with sorted_timestamps
                    signal_series = pd.Series(index=time_series_data[signal_key]['times'], 
                                             data=time_series_data[signal_key]['values'])
                    # Reindex to match all timestamps and interpolate missing values
                    signal_series = signal_series.reindex(sorted_timestamps).interpolate()
                    track_df[signal_key] = signal_series
                
                # Plot the data
                st.line_chart(track_df.set_index('Time'))
                st.write(f"Track: {all_tracks_data[track_id]['name']} (ID: {track_id})")
    
    # Process and send data in real-time
    for time_value in sorted_timestamps:
        # Prepare the combined message with all tracks at this timestamp
        message = {
            "caseid": caseid,
            "time": time_value,
            "tracks": {},
            "age": int(patient_details.get("age", 0)) if not pd.isna(patient_details.get("age", 0)) else 0,
            "sex": str(patient_details.get("sex", "Unknown")),
            "department": str(patient_details.get("department", "Unknown")),
            "lab_results": mapped_lab_results,
        }
        
        # Add all track data for this timestamp
        for track_id, track_data in grouped_by_time[time_value].items():
            message["tracks"][track_id] = {
                "name": track_data['track_name'],
                "signals": track_data['signals']
            }
        
        # ✅ Send Data to Kafka
        try:
            producer_kafka.send("case_tracking", message)
        except Exception as e:
            st.sidebar.error(f"❌ Kafka send failed: {e}")
        
        # ✅ Show Data in Real-Time
        data_placeholder.write(f"⏱️ Time: {time_value} | 📊 Tracks: {len(message['tracks'])} | Signals: {sum(len(t['signals']) for t in message['tracks'].values())}\n")
        
        # Process alerts for the combined message
        process_alerts_by_id(message)
        
        # Simulate real-time streaming with a short delay
        time.sleep(0.5)
    
    # Create a combined view of all vital signs
    st.subheader("🔄 Combined View of All Vital Signs")
    
    # Create a DataFrame for all signals
    all_signals_df = pd.DataFrame()
    all_signals_df['Time'] = sorted_timestamps
    
    # Select important vital signs to show in the combined view
    important_signals = [
        "Heart Rate", "Blood Pressure Systolic", "Blood Pressure Diastolic", 
        "Oxygen Saturation", "Respiratory Rate", "Temperature"
    ]
    
    # Add each important signal if available
    for signal_key, signal_data in time_series_data.items():
        if any(vital in signal_key for vital in important_signals):
            # Create a full time series aligned with sorted_timestamps
            signal_series = pd.Series(index=signal_data['times'], data=signal_data['values'])
            # Reindex to match all timestamps and interpolate missing values
            signal_series = signal_series.reindex(sorted_timestamps).interpolate()
            all_signals_df[signal_key] = signal_series
    
    # Plot the combined data if we have any important signals
    if len(all_signals_df.columns) > 1:  # More than just the Time column
        st.line_chart(all_signals_df.set_index('Time'))
    else:
        st.info("No important vital signs available for combined view")
    
    print(f"✅ Completed processing {len(sorted_timestamps)} timestamps for Case ID {caseid}")
    time.sleep(2) 

# ✅ Main Loop for Case Tracking
def main_interface():
    """Main Streamlit interface for user interaction."""
    st.set_page_config(layout="wide", page_title="🏥 VitalDB Real-Time Monitor")
    st.title("🏥 VitalDB Real-Time Patient Monitoring")
    
    # Add description of the new visualization feature
    st.markdown("""
    ### 📈 Multi-Track Visualization
    This application now displays all patient vital signs simultaneously with respect to time, 
    allowing you to see the relationship between different measurements over the monitoring period.
    
    **Features:**
    - View all tracks for a patient in organized tabs
    - See how vital signs change over time in relation to each other
    - Combined view of critical vital signs for quick assessment
    - Real-time data processing and alerts
    """)

    # 🔹 Sidebar for User Inputs
    st.sidebar.header("👤 Patient Selection")

    # **Case ID Input**
    caseid = st.sidebar.text_input(
        "Enter Case ID (e.g., 1, 2, ...)", value="1"
    )  # Default to '1'

    if not caseid or not caseid.isdigit():
        st.sidebar.warning("⚠️ Please enter a valid numeric Case ID.")
        return
    
    # Add case information if available
    patient_info = cases_df[cases_df["caseid"] == int(caseid)].to_dict(orient="records")
    if patient_info:
        patient = patient_info[0]
        st.sidebar.markdown(f"""
        **Patient Information:**
        - Age: {patient.get('age', 'Unknown')}
        - Sex: {patient.get('sex', 'Unknown')}
        - Department: {patient.get('department', 'Unknown')}
        """)

    # Add explanation about the visualization
    st.sidebar.markdown("""
    **Visualization Guide:**
    1. Track tabs - View each track's signals separately
    2. Combined view - See critical vital signs together
    3. Real-time data - Monitor current values
    """)

    if st.sidebar.button("🚀 Start Multi-Track Visualization"):
        with st.spinner("Loading patient data and preparing visualization..."):
            fetch_track_data(caseid)
    else:
        st.info("✨ Enter a Case ID and click 'Start Multi-Track Visualization' to see all vital signs over time.")

if __name__ == "__main__":
    main_interface()
