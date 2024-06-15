from flask import Flask, jsonify, render_template, request, redirect, url_for

from Recommendations.getRecommendations import getRecommendations
from Services.FileWriter import CSVWriter
from Services.Movies import Movies
from Services.RatingService import RatingsProducer
from Services.RatingsConsumer import RatingsConsumer

app = Flask(__name__)

producer = RatingsProducer()
consumer = RatingsConsumer()
recommendations = getRecommendations()
csv_writer = CSVWriter('ratings.csv', ['userId', 'movieId', 'rating'])  # Initialize the CSVWriter


@app.route('/', methods=['GET'])
def get_movies():
    movies_instance = Movies()
    movies = movies_instance.getMovies()
    return render_template("Movies.html",  movies=movies)


@app.route('/recommendations', methods=['GET'])
def show_recommendations():
    # check recommendations from redis
    data = recommendations.checkRecommendations()
    return render_template("recommendations.html", recommendations=data)


@app.route('/rate_movie')
def rate_movie():
    movieId = request.args.get('movieId')
    title = request.args.get('title')
    genre = request.args.get('genre')
    return render_template('rate_movie.html', movieId=movieId, title=title, genre=genre)


@app.route('/details_movie')
def details_movie():
    movieId = request.args.get('movieId')
    title = request.args.get('title')
    genre = request.args.get('genre')

    return render_template('details_movie.html', movieId=movieId, title=title, genre=genre)


@app.route('/submit_rating', methods=['POST'])
def submit_rating():
    userid = request.form['userId']
    movieId = request.form['movieId']
    rating = request.form['rating']
    print(userid, movieId, rating)
    # Send message to Kafka
    message = {'userId': userid, 'movieId': movieId, 'rating': rating}
    producer.send_message('ratings', userid, message)

    # Consume the message immediately after it's been produced
    try:
        messages = consumer.consume_messages()
        # Write the ratings into a ratings.csv file
        csv_writer.write_to_csv(messages)
        print(messages)
        # Delete the recommendations existing after putting new ratings
        recommendations.deleteRecommendations()
    except Exception as e:
        print('Error in consuming or writing messages in csv:', str(e))
        return jsonify({'error': str(e)}), 500

    return redirect(url_for('get_movies'))


if __name__ == '__main__':
    app.run(threaded=True)
