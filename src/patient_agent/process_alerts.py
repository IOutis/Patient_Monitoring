from kafka import KafkaConsumer
import json

def process_alerts_by_id(patient_id):
    """Consume alerts from Kafka and return them as structured data."""
    consumer = KafkaConsumer(
        "alerts_topic",
        bootstrap_servers="localhost:9092",
        auto_offset_reset="latest",
        value_deserializer=lambda x: json.loads(x.decode("utf-8"))
    )

    print("\n📡 Listening for alerts from Kafka...\n")

    for message in consumer:
        alert = message.value
        if(str(alert['caseid'])==str(patient_id)):
            
            print(f"\n📥 Received Alert Data: {alert}")
            

            formatted_alert = f"""
            🚨 **ALERT: {alert['tname']} exceeded threshold!**
            🔹 **Patient ID**     : {alert['caseid']}
            🔹 **Time Recorded**  : {alert['time']} sec
            🔹 **Value**          : {alert['value']} {alert['unit']}
            🔹 **Expected Range** : {alert['min_value']} - {alert['max_value']} {alert['unit']}
            🔹 **Department**     : {alert['department']}
            """

            print(formatted_alert)
            return formatted_alert  # Return formatted alert for CrewAI
        else:
            return "No alerts for this patient"
