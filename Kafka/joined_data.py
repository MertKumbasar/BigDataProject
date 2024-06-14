from pymongo import MongoClient

client = MongoClient('mongodb://localhost:27017/')
db = client['CENG_476']

pipeline = [
    {
        "$lookup": {
            "from": "weather_monthly_means",  # Name of the weather collection
            "localField": "month",           # Field in fishing_data_processed collection
            "foreignField": "month",         # Field in weather_daily_means collection
            "as": "weather_data"            # Alias for the joined data
        }
    },
    {
        "$unwind": "$weather_data" 
    },
    {
        "$group": {
            "_id": "$month",
            "fishing_data_processed": {"$first": "$total"},
            "weather_data": {"$push": "$weather_data"}
        }
    },
    {
        "$project": {
            "_id": 0,  
            "month": "$_id",  # Include date field
            "fishing_data_processed": 1,  # Include "total" field from fishing_data_processed
            "weather_data": 1  # Include weather data
        }
    }
]

result_collection = db['joined_data']  # New collection for the joined result
result_collection.delete_many({})

joined_data = list(db.fishing_data_processed.aggregate(pipeline))

result_collection.insert_many(joined_data)

print("Joined data inserted into the 'joined_data' collection.")