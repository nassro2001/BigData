import json

from pyspark.sql import SparkSession
from pyspark.ml.recommendation import ALS
from pyspark.sql import functions as F
from pymongo import MongoClient


from Recommendations.RecommProducer import RecommendationsProducer


class MovieRecommender:
    def __init__(self):
        # Create SparkSession
        self.spark = SparkSession.builder.appName("MovieRecommenderALS").getOrCreate()
        self.model = None

        #Connect to mongoDB
        self.client = MongoClient('mongodb://localhost:27017/', serverSelectionTimeoutMS=5000)
        self.client.server_info()  # Trigger exception if unable to connect
        self.db = self.client['BigData']
        self.collection = self.db['moviesList']

    def fetch_movie_details(self, movie_id):
        movie = self.collection.find_one({"movieId": movie_id})
        if movie:
            return movie["title"], movie["genre"]
        return None, None

    def Recommend(self):

        try:
            # Load dataset
            data = self.spark.read.csv("ratings.csv", header=True, inferSchema=True)
            # Split data into training and test sets
            train, test = data.randomSplit([0.8, 0.2], seed=42)

            # Build recommendation model using ALS
            als = ALS(maxIter=5, regParam=0.01, userCol="userId", itemCol="movieId", ratingCol="rating",
                      coldStartStrategy="drop")
            model = als.fit(train)

            # Generate predictions
            predictions = model.transform(test)

            predictions.show()

            # take high predictions
            # Filter predictions greater than 3 and remove duplicates by taking the max prediction for each movieId
            high_predict = (predictions.filter("prediction > 3")
                            .groupBy("movieId")
                            .agg(F.max("prediction").alias("prediction"))
                            .orderBy("prediction"))

            # Fetch movie details from MongoDB, combine them and push them in an array
            results = []
            for row in high_predict.collect():
                movie_id = row["movieId"]
                prediction = row["prediction"]
                title, genre = self.fetch_movie_details(movie_id)
                if title and genre:
                    results.append({"movieId": movie_id, "title": title, "genre": genre, "prediction": prediction})

            # Convert to JSON format for sending to Kafka or other uses
            json_predictions = [json.dumps(result) for result in results]

            # instance of the producer
            producer = RecommendationsProducer()
            # produce in the kafka cluster
            for json_str in json_predictions:
                producer.send_message("movies",  json_str)

        finally:
            self.spark.stop()
