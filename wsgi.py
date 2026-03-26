"""
WSGI entry point for Railway deployment.
Imports and initializes Flask app from V5.py.
"""
import sys
import os
import traceback

print("[WSGI] Starting initialization...", flush=True)

try:
    print("[WSGI] Importing V5 module...", flush=True)
    from V5 import app
    print("[WSGI] V5 module imported successfully", flush=True)
    print(f"[WSGI] Flask app: {app}", flush=True)
    print(f"[WSGI] App name: {app.name}", flush=True)

    # Check routes
    routes = [str(rule) for rule in app.url_map.iter_rules() if not str(rule).startswith('/static')]
    print(f"[WSGI] Routes registered: {len(routes)}", flush=True)
    for route in routes[:5]:
        print(f"[WSGI]   - {route}", flush=True)

except Exception as e:
    print(f"[WSGI] ERROR importing V5: {e}", flush=True)
    traceback.print_exc()
    sys.exit(1)

print("[WSGI] Initialization complete - app ready for gunicorn", flush=True)

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 8080))
    print(f"[WSGI] Running on http://0.0.0.0:{port}", flush=True)
    app.run(host="0.0.0.0", port=port, debug=False)
