"""
WSGI entry point for Railway deployment.
Dynamically creates Flask app and loads routes from V5.
"""
import os
import sys
from flask import Flask, jsonify, request as flask_request, send_from_directory
from flask_cors import CORS

# Create Flask app
_DIR = os.path.dirname(os.path.abspath(__file__))
app = Flask(__name__, static_folder=_DIR)
CORS(app)

# Import V5 to set up routes
# Note: Routes are defined in main_web() in V5.py
# This is a minimal WSGI app - you may need to refactor routes into this file
if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.environ.get("PORT", 8080)), debug=False)
