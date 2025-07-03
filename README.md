# ICU Patient Monitoring Simulation

This project simulates a real-time ICU patient monitoring environment using actual medical data and a distributed streaming system. It models how vitals and lab results are ingested, processed, and tracked in a hospital-like scenario.

> 🏥 Simulated with: Python • Apache Kafka • Pandas • VitalDB API

---

## 🚀 Key Features

- 💾 **Patient Case Loader** – Reads and processes case metadata from structured Excel/CSV files
- 🔄 **Real-Time Streaming** – Simulates live patient vitals using Kafka producers
- 🧪 **Lab Result Integration** – Streams lab events alongside vital signs
- 📊 **Multichannel Signal Handling** – Supports multiple signals per patient (e.g., HR, BP, SpO₂)
- ⏱️ **Configurable Time Control** – Simulates time progression across sessions

---

## 📦 Tech Stack

| Component         | Tech Used            |
|------------------|----------------------|
| Data Ingestion    | Python (pandas, openpyxl) |
| Streaming Layer   | Apache Kafka         |
| Data Source       | [VitalDB API](https://vitaldb.net) |
| Messaging Format  | JSON over Kafka topics |

---

## 🧪 How It Works

1. **Metadata Loader**: Loads patient and case data from Excel files.
2. **Vital Signal Fetcher**: Pulls multi-channel signal data per patient from the VitalDB API.
3. **Kafka Producers**: Send metadata, vitals, and lab results in simulated real time to Kafka topics.
4. **Consumers** *(planned)*: Downstream services (like dashboards or alert systems) can subscribe and visualize the data.

---

## 🚀 Getting Started

> ✅ Kafka must be running locally or in Docker.

To run the project run the file 
gpt_stream_lit.py present in src/patient_agent
