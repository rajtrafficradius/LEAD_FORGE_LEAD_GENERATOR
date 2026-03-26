"""WSGI entry point - Flask app defined here"""
import os
import time
from flask import Flask, jsonify, send_from_directory

# Create Flask app
_DIR = os.path.dirname(os.path.abspath(__file__))
app = Flask(__name__, static_folder=_DIR)

# Configure CORS
@app.after_request
def add_cors_headers(response):
    response.headers['Access-Control-Allow-Origin'] = '*'
    response.headers['Access-Control-Allow-Methods'] = 'GET, POST, OPTIONS'
    response.headers['Access-Control-Allow-Headers'] = 'Content-Type'
    return response

# Routes
@app.route("/")
def serve_index():
    try:
        return send_from_directory(_DIR, "index.html")
    except Exception as e:
        return f"Error: {e}", 500

@app.route("/health")
def health():
    return jsonify({"status": "ok", "version": "V5.7"})

@app.route("/<path:filename>")
def serve_static(filename):
    safe_ext = {".png", ".jpg", ".jpeg", ".gif", ".svg", ".ico", ".css", ".js", ".webp"}
    if os.path.splitext(filename)[1].lower() in safe_ext:
        try:
            return send_from_directory(_DIR, filename)
        except:
            return "Not found", 404
    return "Not found", 404

@app.route("/industries")
def get_industries():
    # Import only when needed to avoid startup issues
    try:
        from V5 import INDUSTRY_KEYWORDS
        return jsonify({"industries": list(INDUSTRY_KEYWORDS.keys())})
    except:
        return jsonify({"industries": ["Electrician", "Plumber", "Photographer"]})

@app.route("/api/credits")
def get_credits():
    services = {
        "apollo": {
            "service": "Apollo",
            "status": "offline",
            "used": 0,
            "total": 1000,
            "pct_remaining": 100,
            "searches_remaining": 1000
        },
        "lusha": {
            "service": "Lusha",
            "status": "offline",
            "used": 0,
            "total": 1000,
            "pct_remaining": 100,
            "searches_remaining": 1000
        },
        "semrush": {
            "service": "SEMrush",
            "status": "offline",
            "used": 0,
            "total": 50000,
            "pct_remaining": 100,
            "searches_remaining": 50000
        },
        "serpapi": {
            "service": "SerpAPI",
            "status": "offline",
            "used": 0,
            "total": 10000,
            "pct_remaining": 100,
            "searches_remaining": 10000
        },
        "openai": {
            "service": "OpenAI",
            "status": "offline",
            "used": 0,
            "total": 5000,
            "pct_remaining": 100,
            "searches_remaining": 5000
        }
    }
    data = {
        "services": services,
        "total_searches_remaining": 77000,
        "timestamp": time.time(),
        "cached": False,
        "alerts": []
    }
    return jsonify(data)

@app.route("/api/credits/refresh", methods=["POST"])
def refresh_credits():
    services = {
        "apollo": {"service": "Apollo", "status": "offline", "used": 0, "total": 1000, "pct_remaining": 100, "searches_remaining": 1000},
        "lusha": {"service": "Lusha", "status": "offline", "used": 0, "total": 1000, "pct_remaining": 100, "searches_remaining": 1000},
        "semrush": {"service": "SEMrush", "status": "offline", "used": 0, "total": 50000, "pct_remaining": 100, "searches_remaining": 50000},
        "serpapi": {"service": "SerpAPI", "status": "offline", "used": 0, "total": 10000, "pct_remaining": 100, "searches_remaining": 10000},
        "openai": {"service": "OpenAI", "status": "offline", "used": 0, "total": 5000, "pct_remaining": 100, "searches_remaining": 5000}
    }
    data = {
        "services": services,
        "total_searches_remaining": 77000,
        "timestamp": time.time(),
        "cached": False,
        "alerts": []
    }
    return jsonify(data)

@app.route("/generate", methods=["POST"])
def generate():
    return jsonify({"error": "Lead generation not available in this deployment"}), 503

@app.route("/status/<job_id>")
def get_status(job_id):
    return jsonify({"error": "Job not found"}), 404

@app.route("/cancel", methods=["POST"])
def cancel():
    return jsonify({"status": "no active job"})

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 8080))
    app.run(host="0.0.0.0", port=port, debug=False)
