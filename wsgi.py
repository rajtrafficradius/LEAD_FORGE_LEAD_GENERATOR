"""
WSGI entry point for Railway deployment.
Simple import of Flask app from V5.py.
"""
import sys
import os

try:
    from V5 import app
except ImportError as e:
    print(f"CRITICAL: Failed to import V5: {e}", file=sys.stderr, flush=True)
    import traceback
    traceback.print_exc()
    sys.exit(1)
except Exception as e:
    print(f"CRITICAL: Unexpected error importing V5: {e}", file=sys.stderr, flush=True)
    import traceback
    traceback.print_exc()
    sys.exit(1)

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 8080))
    app.run(host="0.0.0.0", port=port, debug=False)
