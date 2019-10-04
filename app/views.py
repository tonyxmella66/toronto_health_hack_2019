from flask import render_template
from app import app

# Will manage the html files when being rendered
@app.route('/')
def index():
    return render_template("index.html")