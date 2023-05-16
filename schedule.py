import datetime
import pymongo

client = pymongo.MongoClient("mongodb://<user>:<password>@<host>:<port>/<db_namespace>")
   
def save_data_to_mongo(data):
    db = client["<db_namespace>"]
    collection = db["test_data"]
    response = collection.insert_one(data)

if __name__ == "__main__":
    date_time = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    data = {}
    data["text"] = "Hello World"
    data["date_time"] = date_time
    save_data_to_mongo(data)