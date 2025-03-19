import requests
import pandas as pd
import io
import time
import json
from kafka import KafkaProducer
import random

# 🔹 Kafka Producer Setup
producer = KafkaProducer(
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


# 🔹 Keep Track of Used `tid` to Avoid Repetitions
used_tracks = set()

# 🔹 Signal name mapping (tname_mapping)
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
def fetch_track_data():
    global used_tracks

    # **Pick a new track every time**
    available_tracks = track_list[~track_list["tid"].isin(used_tracks)]
    
    if available_tracks.empty:
        print("✅ All tracks used! Resetting history...")
        used_tracks.clear()
        available_tracks = track_list  # Reset the dataset

    random_row = available_tracks.sample(n=1)
    track_id = random_row['tid'].values[0]
    caseid = str(random_row['caseid'].values[0])

    # Mark this track as used
    used_tracks.add(track_id)

    # **🔹 API Fetch with Retry Logic**
    max_retries = 3  # Try 3 times before giving up
    retry_delay = 5   # Wait 5 seconds between retries
    track_data = None

    for attempt in range(max_retries):
        response = requests.get(f"https://api.vitaldb.net/{track_id}", timeout=30)

        if response.status_code == 200 and response.content:
            data = response.content.decode('utf-8')
            track_data = pd.read_csv(io.StringIO(data))
            break  # Success, exit retry loop
        else:
            print(f"⚠️ Attempt {attempt+1}/{max_retries} failed for {track_id}. Retrying in {retry_delay}s...")
            time.sleep(retry_delay)

    if track_data is None or track_data.empty:
        print(f"❌ Failed to fetch valid data for {track_id}. Skipping this track...")
        return  # Skip this iteration and fetch a new track

    print(f"✅ Fetched Data for {track_id} - {track_data.shape[0]} rows")  # Debugging

    # **Filter out invalid/missing values**
    track_data = track_data.dropna()  # Remove NaN values
    track_data = track_data[track_data.iloc[:, 0] > 0]  # Remove time=0 rows
    track_data = track_data[track_data.iloc[:, 1] > 0]  # Remove value=0 rows

    # **Check if DataFrame is Empty After Filtering**
    if track_data.empty:
        print(f"⚠️ No valid data found for track {track_id}. Skipping...")
        return  # Skip this iteration and fetch a new track

    # Fetch patient details
    patient_info = cases_df[cases_df["caseid"] == int(caseid)].to_dict(orient="records")
    patient_details = patient_info[0] if patient_info else {}

    # Fetch lab results for the case ID
    lab_results = labs_df[labs_df["caseid"] == int(caseid)].to_dict(orient="records")

    # **Map lab results using lab_results_map**
    mapped_lab_results = {}
    for lab in lab_results:
        lab_name = lab.get("name", "")
        lab_result = lab.get("result", "")
        if lab_name in lab_results_map:  # Check if the lab name exists in the mapping
            mapped_lab_name = lab_results_map[lab_name]  # Get the mapped name
            mapped_lab_results[mapped_lab_name] = lab_result  # Add to the new dictionary
        else:
            mapped_lab_results[lab_name] = lab_result  # Keep the original name if no mapping exists

    # **Select One Random Row Safely**
    row = track_data.sample(n=1).iloc[0]

    time_value = float(row.iloc[0]) if not pd.isna(row.iloc[0]) else 0.0

    for signal_name in track_data.columns[1:]:
        signal_value = float(row[signal_name]) if not pd.isna(row[signal_name]) else 0.0

        # Map signal name to user-friendly name
        mapped_signal_name = tname_mapping.get(signal_name, signal_name)

        # Prepare the message
        message = {
            "caseid": caseid,
            "tname": mapped_signal_name,  # Use mapped signal name
            "tid": track_id,
            "time": time_value,
            "value": signal_value,
            "age": int(patient_details.get("age", 0)) if not pd.isna(patient_details.get("age", 0)) else 0,
            "sex": str(patient_details.get("sex", "Unknown")),
            "department": str(patient_details.get("department", "Unknown")),
            "lab_results": mapped_lab_results,  # Use mapped lab results
        }

        # Send message to Kafka
        producer.send("pathway_topic", message)
        print(f"📤 Sent Track Data: {message}")

    # **Immediately switch to a new track on next iteration**
    time.sleep(3)  # Short delay before next iteration
    
# 🔹 Run Streaming (Keep Switching Tracks)
while True:
    fetch_track_data()
