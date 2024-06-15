from pymongo import MongoClient, errors


class Movies:
    def __init__(self):
        try:
            self.client = MongoClient('mongodb://localhost:27017/', serverSelectionTimeoutMS=5000)
            self.client.server_info()  # Trigger exception if unable to connect
            self.db = self.client['BigData']
            self.collection = self.db['moviesList']
        except errors.ServerSelectionTimeoutError as err:
            print(f"Could not connect to MongoDB: {err}")
            self.client = None

    def getMovies(self):
        if not self.client:
            return []
        try:
            movies = list(self.collection.find({}, {'_id': 0}))
            return movies
        except errors.PyMongoError as err:
            print(f"An error occurred: {err}")
            return []
