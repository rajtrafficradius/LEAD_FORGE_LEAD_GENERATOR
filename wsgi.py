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

# Basic routes
@app.route("/")
def index():
    """Serve index.html"""
    try:
        return send_from_directory(_DIR, "index.html")
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route("/health")
def health():
    """Health check endpoint"""
    return jsonify({"status": "ok", "app": "LEAD_FORGE_LEAD_GENERATOR"}), 200

@app.errorhandler(404)
def not_found(error):
    """Handle 404 errors"""
    return jsonify({"error": "Not Found", "path": request.path}), 404

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.environ.get("PORT", 8080)), debug=False)
