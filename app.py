from flask import Flask, request, jsonify
from kafka import KafkaProducer
import json

app = Flask(__name__)

# Set up Kafka Producer
kafka_producer = KafkaProducer(bootstrap_servers='localhost:9092',
                               value_serializer=lambda v: json.dumps(v).encode('utf-8'))


@app.route('/send', methods=['POST'])
def send_data():
    try:
        # Check if the Content-Type header is set to 'application/json'
        if request.headers.get('Content-Type') != 'application/json':
            return jsonify({"error": "Content-Type must be application/json"}), 415

        # Get JSON data from the request
        data = request.get_json()
        if data is None:
            return jsonify({"error": "No JSON data found in the request"}), 400

        print(f"Received data: {data}")  # Debug: Print received data

        # Send data to Kafka topic 'market_data'
        kafka_producer.send('market_data', value=data)
        kafka_producer.flush()

        return jsonify({"message": "Data sent to Kafka"}), 200

    except Exception as e:
        print(f"Error sending data: {e}")
        return jsonify({"error": str(e)}), 500


if __name__ == "__main__":
    app.run(port=5001, debug=True)
