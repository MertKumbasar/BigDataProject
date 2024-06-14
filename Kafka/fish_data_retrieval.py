from pymongo import MongoClient
import requests
from datetime import datetime, timedelta
from kafka import KafkaProducer
import json

def get_monthly_data(producer, year, month, end_day=None):
    url = 'https://gateway.api.globalfishingwatch.org/v3/events?offset=0&limit=10'
    headers = {
        'Authorization': 'Bearer eyJhbGciOiJSUzI1NiIsInR5cCI6IkpXVCIsImtpZCI6ImtpZEtleSJ9.eyJkYXRhIjp7Im5hbWUiOiJCaWcgRGF0YSBQcm9qZWN0IiwidXNlcklkIjozNDQ5NSwiYXBwbGljYXRpb25OYW1lIjoiQmlnIERhdGEgUHJvamVjdCIsImlkIjoxNTE0LCJ0eXBlIjoidXNlci1hcHBsaWNhdGlvbiJ9LCJpYXQiOjE3MTUxMDg5NDAsImV4cCI6MjAzMDQ2ODk0MCwiYXVkIjoiZ2Z3IiwiaXNzIjoiZ2Z3In0.D2Soyo47Zb-gWaN_-Wz_2G9JTdyP6Nu2t-32BJKSJbA3ws22FEBUW2a0ktCGwiXCJzD5eJW4MabjCp0vKUOWaxu3XWLC_ynvRDWecQy8IRh9aVpzesM5mSiqzKsKDndcX_mJs3UJzrV2tqsalcsLMzBwJKiyvjAg58_d7cCf1srFRouqGN2pML_v5I4u_Lx-E1XOl9DbJ2SZhjLjy2r91gLrcyE2pNMAvZwN8xkH9lL40zNC0V47XU41jx7ZGxE7w5JDIqsGu5QZmyoKDaMmBnKFJsCX8vqypMdKFTBFrEKT8l_ekAgce4OdWsHUmAAPWh6eUFLfrtCvcbEfa6D1oh5hHih9Nay153AcqCXoVCsEpc01SzgEesjKFG8RPzsjGesDT_Gwt3KumEd5SJbIPeNdS8B8UVKByR_AXAIJggqjV6ARRhO-VGJDOTZZJCXawDEDEfTswwKiaV1wjSE4Px8g5i5uB81z5Z01p7yWTD3mxqyg0TVrWlG5Lly16ZJe',
        'Content-Type': 'application/json'
    }
    
    start_date = f"{year}-{month:02d}-01"
    if end_day:
        end_date = f"{year}-{month:02d}-{end_day:02d}"
    else:
        end_date = datetime(year, month, 1) + timedelta(days=32)
        end_date = f"{end_date.year}-{end_date.month:02d}-01"

    data = {
        "datasets": ["public-global-fishing-events:latest"],
        "startDate": start_date,
        "endDate": end_date,
        "geometry": {
            "type": "Polygon",
            "coordinates": [
                [
                    [41.068887774169625, 27.998046875],
                    [46.92025531537451, 27.998046875],
                    [46.92025531537451, 41.6162109375],
                    [41.068887774169625, 41.6162109375],
                    [41.068887774169625, 27.998046875]
                ]
            ]
        }
    }

    response = requests.post(url, headers=headers, json=data)

    if response.status_code == 200 or response.status_code == 201:
        print(f"Data retrieval successful for {start_date} to {end_date}")
        json_data = response.json()

        keys_to_delete_main = ['start', 'end', 'id', 'type', 'position', 'regions', 'boundingBox', 'distances', 'fishing']
        keys_to_delete_vessel = ['id', 'ssvid', 'type', 'publicAuthorizations']

        for entry in json_data["entries"]:
            for key in keys_to_delete_main:
                del entry[key]

            for key in keys_to_delete_vessel:
                del entry['vessel'][key]

        from_date = json_data["metadata"]["dateRange"]["from"]
        month_only = from_date[:7]  
        
        new_json_data = {
            "month": month_only,
            **json_data
        }
        
        # Remove metadata
        del new_json_data["metadata"]
        del new_json_data["limit"]
        del new_json_data["offset"]
        del new_json_data["nextOffset"]

        producer.send('fishing_data', new_json_data)
        print("Data published to Kafka topic: fishing_data")
    else:
        print(f"Data retrieval failed for {start_date} to {end_date} with status code:", response.status_code)

def get_and_store_monthly_data(producer, start_year, start_month, end_year, end_month, end_day=None):
    for year in range(start_year, end_year + 1):
        for month in range(1, 13):
            if year == start_year and month < start_month:
                continue
            if year == end_year and month > end_month:
                break
            if year == end_year and month == end_month:
                get_monthly_data(producer, year, month, end_day)
            else:
                get_monthly_data(producer, year, month)

# Kafka Producer
producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

# Data from December 2023 to May 30, 2024
start_year = 2023
start_month = 12
end_year = 2024
end_month = 5
end_day = 25

get_and_store_monthly_data(producer, start_year, start_month, end_year, end_month, end_day)
