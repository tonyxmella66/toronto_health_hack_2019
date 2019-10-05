import os

import numpy as np

import joblib
from flask import Flask, render_template, request, jsonify
from google.cloud import bigquery
from tensorflow import keras


def create_app(test_config=None):
    # create and configure the app
    app = Flask(__name__, instance_relative_config=True)

    if test_config is None:
        # load the instance config, if it exists, when not testing
        app.config.from_pyfile('config.py', silent=True)
    else:
        # load the test config if passed in
        app.config.from_mapping(test_config)

    # ensure the instance folder exists
    try:
        os.makedirs(app.instance_path)
    except OSError:
        pass

    @app.route('/', methods=['GET', 'POST'])
    def index():
        if request.method == 'POST':
            pass
        return render_template("index.html")

    @app.route('/search', methods=['GET', 'POST'])
    def search_id():
        model = keras.models.load_model('model.h5')
        X, y, s = joblib.load('data.joblib')
        cv = joblib.load('cv.joblib')
        stay_id = int(request.form['stay_id'])
        index = s.index(stay_id)
        X = X[index]
        X = X.reshape(1,X.shape[0])
        y = y[index]
        preds = model.predict(X)
        preds_dichot = (preds > 0.05) * 1
        predictions_text = cv.inverse_transform(preds_dichot[0])
        predictions_text = predictions_text[0].tolist()
        print(predictions_text)
        if request.method == 'POST':
            return render_template("index.html", results=predictions_text)

    return app
