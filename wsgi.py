"""
WSGI entry point for Railway deployment.
Imports Flask app from V5.py module-level 'app' object.
"""
from V5 import app

if __name__ == "__main__":
    import os
    port = int(os.environ.get("PORT", 8080))
    app.run(host="0.0.0.0", port=port, debug=False)
