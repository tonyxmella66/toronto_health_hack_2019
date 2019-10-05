import os
from google.cloud import bigquery
from flask import Flask
from flask import render_template, request

def create_app(test_config=None):
    # create and configure the app
    app = Flask(__name__, instance_relative_config=True)
    app.config.from_mapping(
        SECRET_KEY='dev',
        DATABASE=os.path.join(app.instance_path, 'flaskr.sqlite'),
    )

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
            client = bigquery.Client()
            example_query = client.query('SELECT * FROM `physionet-data.mimiciii_clinical.prescriptions` WHERE DRUG_TYPE = "MAIN" LIMIT 20')
            query_dataframe = example_query.to_dataframe()

        return render_template("index.html")

    return app