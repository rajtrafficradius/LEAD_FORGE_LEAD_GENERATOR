"""WSGI entry point - Flask app defined here"""
import os
import time
import csv as _csv
import uuid as _uuid
import threading
from flask import Flask, jsonify, send_from_directory, request

# Create Flask app
_DIR = os.path.dirname(os.path.abspath(__file__))
app = Flask(__name__, static_folder=_DIR)

# Job management
_jobs = {}

class JobState:
    def __init__(self):
        self.progress = 0
        self.status_text = "Starting..."
        self.state = "running"
        self.logs = []
        self.log_cursor = 0
        self.leads = []
        self.top_csv = ""
        self.all_csv = ""
        self.error = ""
        self.pipeline = None
        self.api_usage = {}

# Configure CORS
@app.after_request
def add_cors_headers(response):
    response.headers['Access-Control-Allow-Origin'] = '*'
    response.headers['Access-Control-Allow-Methods'] = 'GET, POST, OPTIONS'
    response.headers['Access-Control-Allow-Headers'] = 'Content-Type'
    return response

@app.errorhandler(404)
def not_found(error):
    return jsonify({"error": "Not found"}), 404

@app.errorhandler(500)
def internal_error(error):
    return jsonify({"error": "Internal server error"}), 500

@app.errorhandler(Exception)
def handle_exception(error):
    return jsonify({"error": str(error)}), 500

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
    try:
        from V5 import LeadGenerationPipeline

        data = request.get_json()
        if not data:
            return jsonify({"error": "No data provided"}), 400

        industry = data.get("industry", "")
        country = data.get("country", "AU")
        min_volume = int(data.get("min_volume", 100))
        min_cpc = float(data.get("min_cpc", 1.0))
        max_leads = int(data.get("max_leads", 0))

        if not industry:
            return jsonify({"error": "Industry is required"}), 400

        job_id = str(_uuid.uuid4())[:8]
        job = JobState()
        _jobs[job_id] = job
        output_folder = os.path.join(_DIR, "output", job_id)
        os.makedirs(output_folder, exist_ok=True)

        def progress_cb(pct, status=""):
            job.progress = pct
            if status:
                job.status_text = status

        def log_cb(message):
            job.logs.append(message)

        pipeline = LeadGenerationPipeline(
            industry=industry, country=country, min_volume=min_volume,
            min_cpc=min_cpc, output_folder=output_folder,
            progress_callback=progress_cb, log_callback=log_cb, max_leads=max_leads,
        )
        job.pipeline = pipeline

        def run():
            try:
                job.progress = 1
                job.status_text = "Initializing pipeline..."
                job.logs.append("[SYSTEM] Pipeline initialized, starting Phase 1...")
                result_path = pipeline.run()
                job.api_usage = pipeline._api_counter.copy()
                if pipeline._cancelled:
                    job.state = "cancelled"
                    return
                if result_path and os.path.exists(result_path):
                    with open(result_path, "r", encoding="utf-8") as f:
                        job.top_csv = f.read()
                    with open(result_path, "r", encoding="utf-8") as f:
                        reader = _csv.DictReader(f)
                        for row in reader:
                            job.leads.append({
                                "name": row.get("Name", ""),
                                "company": row.get("Company Name", ""),
                                "domain": row.get("Domain", ""),
                                "role": row.get("Role", ""),
                                "email": row.get("Email", ""),
                                "phone": row.get("Phone Number", ""),
                                "email_type": row.get("Email Type", ""),
                            })
                    for fname in os.listdir(output_folder):
                        if fname.startswith("leads_ALL_") and fname.endswith(".csv"):
                            with open(os.path.join(output_folder, fname), "r", encoding="utf-8") as f:
                                job.all_csv = f.read()
                            break
                    job.state = "done"
                else:
                    job.state = "done" if not pipeline._cancelled else "cancelled"
            except Exception as e:
                job.error = str(e)
                job.state = "error"

        threading.Thread(target=run, daemon=True).start()
        return jsonify({"job_id": job_id})
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route("/status/<job_id>")
def get_status(job_id):
    try:
        job = _jobs.get(job_id)
        if not job:
            return jsonify({"error": "Job not found"}), 404
        new_logs = job.logs[job.log_cursor:]
        job.log_cursor = len(job.logs)
        result = {
            "state": job.state,
            "progress": job.progress,
            "status_text": job.status_text,
            "new_logs": new_logs
        }
        if job.state == "done":
            result["leads"] = job.leads
            result["top_csv"] = job.top_csv
            result["all_csv"] = job.all_csv
            result["api_usage"] = job.api_usage
        if job.state == "error":
            result["error"] = job.error
            result["api_usage"] = job.api_usage
        return jsonify(result)
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route("/cancel", methods=["POST"])
def cancel():
    try:
        for jid in reversed(list(_jobs.keys())):
            j = _jobs[jid]
            if j.state == "running" and j.pipeline:
                j.pipeline.cancel()
                return jsonify({"status": "cancelling"})
        return jsonify({"status": "no active job"})
    except Exception as e:
        return jsonify({"error": str(e)}), 500

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 8080))
    app.run(host="0.0.0.0", port=port, debug=False)
