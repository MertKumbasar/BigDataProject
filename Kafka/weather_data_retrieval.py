import requests
import json
from kafka import KafkaProducer

producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8'),
    max_request_size=10485760

)

url = "https://api.meteomatics.com/2023-12-01T00:00:00.000+03:00--2024-05-31T23:59:59.000+03:00:PT1H/mean_wave_direction:d,msl_pressure_mean_1h:Pa,relative_humidity_2m:p,sea_salt_0p03um_0p5um:ugm3,significant_wave_height:m,t_sea_sfc:C,visibility:m,wind_dir_10m:d,wind_speed_10m:ms,t_10m:C/43.8728239,33.9938077/json?model=mix"
api_key = "eyJhbGciOiJFUzI1NiIsInR5cCI6IkpXVCJ9.eyJ2IjoxLCJ1c2VyIjoic3R1ZGVudF9tZW5la2VfYXllaW1hbCIsImlzcyI6ImxvZ2luLm1ldGVvbWF0aWNzLmNvbSIsImV4cCI6MTcxNzEwNTg1MCwic3ViIjoiYWNjZXNzIn0.Vtwp1gBjLTAay3ALkOOGdy8PgLlHq_mCW0za776LaqXLZX5V1pNofWZP7igAf2iS7LyimvujX2regBlDMrU2CA"

headers = {
    "Authorization": f"Bearer {api_key}"
}

response = requests.get(url, headers=headers)

if response.status_code == 200 or response.status_code == 201:
    print("Request successful")
    data = response.json()

    # Extract and format the data
    formatted_data = {
        "metadata": {
            "datasets": ["public-global-weather-data:v20230526"],
            "dateRange": {
                "from": "2023-12-01",
                "to": "2024-05-25"
            }
        },
        "data": []
    }

    for parameter_data in data['data']:
        parameter = parameter_data['parameter']
        coordinates = parameter_data.get('coordinates', [])

        for coord in coordinates:
            lat = coord.get('lat')
            lon = coord.get('lon')

            dates = []
            for date_value in coord.get('dates', []):
                date = date_value.get('date')
                value = date_value.get('value')
                dates.append({"date": date, "value": value})

            formatted_entry = {
                "parameter": parameter,
                "coordinates": {
                    "lat": lat,
                    "lon": lon,
                    "dates": dates
                }
            }
            formatted_data["data"].append(formatted_entry)

    print("Formatted data:", formatted_data)

    future = producer.send('weather_data', formatted_data)
    result = future.get(timeout=60)  # Data is sent within 60 seconds

    print("Data published to Kafka topic: weather_data")
else:
    print("Request failed with status code:", response.status_code)
