import json
from pymongo import MongoClient
import threading
from kafka import KafkaConsumer

# MongoDB Client
client = MongoClient('mongodb://localhost:27017/')
db = client['CENG_476']

def process_fishing_data(data):
    try:
        collection = db["fishing_data_processed"]
        collection.insert_one(data)
        print("Fishing data stored in MongoDB collection: fishing_data_processed")
    except Exception as e:
        print(f"Error processing fishing data: {e}")

def calculate_daily_mean(data):
    daily_data = {}
    for entry in data:
        date = entry['date'][:10]  # Extract date in YYYY-MM-DD format
        value = entry['value']
        if date not in daily_data:
            daily_data[date] = []
        daily_data[date].append(value)
    daily_mean = {date: sum(values) / len(values) for date, values in daily_data.items()}
    return daily_mean

def calculate_monthly_mean(daily_mean):
    monthly_data = {}
    for date, mean in daily_mean.items():
        month = date[:7]  # Extract year and month in YYYY-MM format
        if month not in monthly_data:
            monthly_data[month] = []
        monthly_data[month].append(mean)
    monthly_mean = {month: sum(values) / len(values) for month, values in monthly_data.items()}
    return monthly_mean

def process_weather_data(data):
    print(f"Received weather data: {data}")
    if 'data' not in data:
        print("No 'data' key in weather data.")
        return

    for parameter_data in data['data']:
        print(f"Processing weather data for parameter: {parameter_data['parameter']}")
        if 'coordinates' not in parameter_data or not parameter_data['coordinates']:
            print("No 'coordinates' key or empty coordinates in parameter data.")
            continue

        coordinates = parameter_data['coordinates']
        if not isinstance(coordinates, list):
            coordinates = [coordinates]

        for coordinates_data in coordinates:
            dates = coordinates_data.get('dates', [])
            if not isinstance(dates, list):
                dates = [dates]

            if not dates:
                print("No 'dates' in coordinates.")
                continue

            daily_mean = calculate_daily_mean(dates)
            monthly_mean = calculate_monthly_mean(daily_mean)

            # Store daily means
            daily_collection = db['weather_daily_means']
            for date, mean in daily_mean.items():
                print(f"Storing daily mean for {date}: {mean}")
                daily_collection.update_one(
                    {'date': date, 'parameter': parameter_data['parameter']},
                    {'$set': {'mean_value': mean}},
                    upsert=True
                )

            # Store monthly means
            monthly_collection = db['weather_monthly_means']
            for month, mean in monthly_mean.items():
                print(f"Storing monthly mean for {month}: {mean}")
                monthly_collection.update_one(
                    {'month': month, 'parameter': parameter_data['parameter']},
                    {'$set': {'mean_value': mean}},
                    upsert=True
                )

        print(f"Processed and stored means for {parameter_data['parameter']}")

def consume_messages(consumer, process_func):
    print(f"Starting consumer: {consumer}")
    try:
        for message in consumer:
            print(f"Consumed message: {message.value}")
            data = message.value
            process_func(data)
    except Exception as e:
        print(f"Error consuming messages: {e}")
    finally:
        consumer.close()

# Kafka Consumers
consumer_fishing = KafkaConsumer(
    'fishing_data',
    bootstrap_servers='localhost:9092',
    auto_offset_reset='earliest',
    enable_auto_commit=True,
    group_id='api_data_consumers',
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)

consumer_weather = KafkaConsumer(
    'weather_data',
    bootstrap_servers='localhost:9092',
    auto_offset_reset='earliest',
    enable_auto_commit=True,
    group_id='api_data_consumers',
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)

try:
    # Consumer threads
    fishing_thread = threading.Thread(target=consume_messages, args=(consumer_fishing, process_fishing_data))
    weather_thread = threading.Thread(target=consume_messages, args=(consumer_weather, process_weather_data))

    fishing_thread.start()
    weather_thread.start()

    fishing_thread.join()
    weather_thread.join()
except Exception as e:
    print(f"Error in main thread: {e}")
finally:
    client.close()
