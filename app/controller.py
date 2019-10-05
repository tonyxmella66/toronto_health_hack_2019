import functools
from flask import (
    Blueprint, flash, g, redirect, render_template, request, session, url_for
)
import functools

bp = Blueprint('index', __name__, url_prefix='/')

