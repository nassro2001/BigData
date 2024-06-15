import json

import redis

from Recommendations.RecommConsumer import recommendationsConsumer
from Recommendations.recommendationsSystem import MovieRecommender


class getRecommendations:
    result = None
    # Set up for redis
    def __init__(self):
        self.redis = redis.Redis(
            host='fleet-jackass-52441.upstash.io',
            port=6379,
            password='AczZAAIncDE1ZGMyMmQzZjQzNTk0Y2JjYTEzOWY1Y2ZmYzM0YzY0NnAxNTI0NDE',
            ssl=True,
            decode_responses=True
        )
        # Instance of the recommendations consumer
        self.consumer = recommendationsConsumer()

    def checkRecommendations(self):

        keys = self.redis.keys("recommendation:*")
        self.result = []
        if keys:
            print("found")
            for key in keys:
                recommendation_json = self.redis.get(key)
                recommendation = json.loads(recommendation_json)
                self.result.append(recommendation)

            return self.result
        else:
            print("not found")
            # We compute the recommendations and we return it after storing it in results
            self.computeRecommendations()
            return self.result

    def computeRecommendations(self):
        # We compute the recommendations by calling it from another class
        recommendations = MovieRecommender()
        recommendations.Recommend()
        # After computing we need to consume the data from kafka and store them in redis for caching
        messages = self.consumer.consume_messages()

        if messages:
            self.store_in_redis(messages)

    def store_in_redis(self, recommendations):
        # Store each recommendation in Redis hash "recommendations"
        for recommendation in recommendations:
            movie_id = recommendation['movieId']
            self.redis.set(f"recommendation:{movie_id}", json.dumps(recommendation))

        print("Recommendations stored in Redis successfully.")

        # after storing them we need directly to get them and display them
        keys = self.redis.keys("recommendation:*")

        for key in keys:
            recommendation_json = self.redis.get(key)
            recommendation = json.loads(recommendation_json)
            self.result.append(recommendation)

    def deleteRecommendations(self):
        # Delete the recommendations
        delete = self.redis.flushdb()
        if delete == 1:
            print(f"data was successfully deleted.")
        else:
            print(f"data does not exist or was already deleted.")
